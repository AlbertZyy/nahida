from __future__ import annotations

__all__ = [
    "PortId",
    "TaskItem",
    "InputDataNotFoundError",
    "InputMissingError",
    "NodeExceptionError",
    "OutputLengthMismatchError",
    "DataAlreadyExistError",
    "Node",
    "Execute",
    "Branch",
    "Repeat",
    "Break",
    "Join",
    "Group"
]

import inspect
from inspect import _ParameterKind as PPK
from typing import Any, overload
from collections.abc import Callable, Iterable, Sequence
from collections import namedtuple
from dataclasses import dataclass, field
from enum import StrEnum


PortId = namedtuple("PortId", ["node_id", "index"])
type PortOrNode = PortId | Node


class FlowCtrl(StrEnum):
    """Workflow control instruction after execution."""

    NONE = "none"
    """Do nothing and be poped from the stack top."""

    REPEAT = "repeat"
    """Require to be re-pushed into the computation stack."""

    BREAK = "break"
    """Require to pop the nearest re-pushed node directly (no execution)
    when the next time it is on the top."""


@dataclass(slots=True, frozen=True)
class TaskItem:
    """Task item for a node.

    See `Node.submit` for details.
    """
    target: Callable | None = None
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    recruit: set[Node] | None = None
    control: FlowCtrl = FlowCtrl.NONE


class InputDataNotFoundError(Exception):
    """Raised when connected input data cannot be found in the context."""


class InputMissingError(Exception):
    """Raised when any required input is missing."""


class NodeExceptionError(Exception):
    """Raised when an error occurs in the node's exception handling."""


class OutputLengthMismatchError(Exception):
    """Raised when number of outputs does not match the number of function
    return _values."""


class DataAlreadyExistError(Exception):
    """Raised when output data already exist in the context."""


class Node(object):
    """Abstract base class for all nodes.

    Nodes are computational units that can be connected to other nodes, and
    are designed with the following interfaces:
      - `submit`: submit execution/scheduling tasks to the controller,
      - `write`: put output values to the context,

    where `submit` is abstract and must be implemented by subclasses, and
    `write` is defaults to putting values into the context dict directly.
    Here `context` is a dictionary of node IDs to the
    corresponding output values.
    """
    def submit(self, context: dict[int, Any]) -> TaskItem:
        """Get a task to be submitted to the task queue.

        A task item is a dataclass containing the following fields:
        - target: the function to be executed. None for no execution tasks.
        - args: the positional arguments.
        - kwargs: the keyword arguments.
        - recruit: the nodes to be recruited, also the downstreams.
        - control: the control instruction after execution.

        where the target, args and kwargs represents an exection task, while
        recruit and control for a scheduling task.

        Args:
            context (dict[int, Any]): The context of the environment.

        Returns:
            TaskItem: The task item to be submitted to the task queue.
        """
        raise NotImplementedError

    def write(self, context: dict[int, Any], values: Any) -> None:
        """Put values into the context.

        Args:
            context (dict[int, Any]): The context of the environment.
            values (Any): The values to be put into the context.
        """
        context[id(self)] = values


def get_inputs(func: Callable) -> Iterable[tuple[str, Any, bool]]:
    """Return the node's input names and defaults."""
    sig = inspect.signature(func)

    for name, param in sig.parameters.items():
        if param.kind in (PPK.POSITIONAL_OR_KEYWORD, PPK.KEYWORD_ONLY):
            has_default = param.default is not param.empty
            yield name, param.default, has_default
        if param.kind in (PPK.POSITIONAL_ONLY, PPK.VAR_POSITIONAL):
            raise TypeError("Positional-only parameters are not supported")


class _ContextReader:
    """Supports read_context in a node.

    Introduce `_values` and `_connects` dicts for defaults and subscriptions.
    """
    _values: dict[str, Any]
    _connects: dict[str, PortId]

    def __init__(self, **kwargs: PortOrNode | Any):
        self._values = {}
        self._connects = {}
        self.subscribe(**kwargs)

    def subscribe(self, **kwargs: PortOrNode | Any) -> None:
        """Set subscriptions for attributes.

        An attribute may take values from other nodes or constants. Use pairs
        like `arg1=node` or `arg1=value` to define the souce of the attribute
        named `arg1`. Use `arg1=node[index]` for source nodes returning a
        tuple, list, dict or any other types that supports __getitem__.

        Args:
            **kwargs (PortId | Node | Any): The attribute-source pairs.
        """
        for name, value in kwargs.items():
            if isinstance(value, PortId):
                self._connects[name] = value
            elif isinstance(value, Node):
                self._connects[name] = PortId(id(value), None)
            else:
                self._values[name] = value

    def unsubscribe(self, *names: str) -> None:
        """Remove subscriptions for attributes."""
        for name in names:
            self._values.pop(name, None)
            self._connects.pop(name, None)

    def __getitem__(self, key: str | int) -> PortId:
        return PortId(id(self), key)

    def read_context(self, context: dict[int, Any], name: str) -> tuple[Any, bool]:
        """Get value for an input from the context."""
        if name in self._connects:
            node_id, index = self._connects[name]

            if node_id in context:
                val = context[node_id]

                if index is not None:
                    val = val[index]

                return val, True

            raise InputDataNotFoundError(
                f"Input {name!r} of {self!r} cannot be found in the context"
            )

        if name in self._values:
            return self._values[name], True

        return None, False


class _Recruiter:
    """Supports routing downstream nodes.

    Introduce `_downstreams` list for downstream nodes.
    `link` and `unlink` are available for manage downstream nodes.
    """
    _downstreams: set[Node]

    def __init__(self, downstream: set[Node] | None = None, /):
        if downstream is None:
            downstream = set()
        self._downstreams = downstream

    def link(self, *other: Node) -> None:
        """Add downstream nodes to be recruited after execution.

        Args:
            *other (Node): The downstream nodes to be added.
        """
        self._downstreams.union(other)

    def unlink(self, *other: Node) -> None:
        """Remove downstream nodes. Do nothing for non-existent nodes.

        Args:
            *other (Node): The downstream nodes to be removed.
        """
        self._downstreams.difference_update(other)

    @property
    def downstreams(self) -> set[Node]:
        return self._downstreams


class Execute(_Recruiter, _ContextReader, Node):
    """Computational Node object."""
    _target: Callable

    def __init__(self, target: Callable, /):
        _ContextReader.__init__(self)
        _Recruiter.__init__(self)
        sig = inspect.signature(target)

        for param in sig.parameters.values():
            if param.kind in (PPK.POSITIONAL_ONLY, PPK.VAR_POSITIONAL):
                raise TypeError("Positional-only parameters are not supported")

        self._target = target

    def __repr__(self) -> str:
        return f"<Node {self._target.__name__} at {hex(id(self))}>"

    def submit(self, context: dict[int, Any] = {}):
        input_kwargs = {}

        for param, _, has_default in get_inputs(self._target):
            val, status = self.read_context(context, param)
            if status:
                input_kwargs[param] = val
                continue
            if has_default:
                continue
            raise InputMissingError(f"No value for input {param!r} in {self!r}")

        return TaskItem(
            target=self._target,
            kwargs=input_kwargs,
            recruit=self.downstreams
        )


class Branch(_ContextReader, Node):
    """Branch the execution based on a condition."""
    _downstreams_true: set[Node]
    _downstreams_false: set[Node]

    def __init__(self, condition: PortOrNode | bool = False, /):
        _ContextReader.__init__(self, condition=condition)
        self._downstreams_true = set()
        self._downstreams_false = set()

    @property
    def true(self):
        """Execute downstream nodes when condition is True."""
        return _Recruiter(self._downstreams_true)

    @property
    def false(self):
        """Execute downstream nodes when condition is False."""
        return _Recruiter(self._downstreams_false)

    def submit(self, context: dict[int, Any] = {}):
        val, status = self.read_context(context, "condition")

        if bool(val) and status:
            return TaskItem(recruit=self._downstreams_true)
        else:
            return TaskItem(recruit=self._downstreams_false)


class Repeat(_Recruiter, _ContextReader, Node):
    """Repeat the execution for multiple times."""
    @overload
    def __init__(self, stop: int | PortOrNode = 1, /): ...
    @overload
    def __init__(self, start: int | PortOrNode, stop: int | PortOrNode, step: int | PortOrNode = 1, /): ...
    def __init__(self, *args):
        if len(args) == 0:
            start, stop, step = 0, 1, 1
        elif len(args) == 1:
            start, stop, step = 0, args[0], 1
        elif len(args) == 2:
            start, stop, step = args[0], args[1], 1
        elif len(args) == 3:
            start, stop, step = args
        else:
            raise TypeError("Invalid arguments")

        _ContextReader.__init__(self, start=start, stop=stop, step=step)
        _Recruiter.__init__(self)
        self._iterator = None
        self._downstreams_stop: set[Node] = set()

    @property
    def stop(self):
        """Execute downstream nodes when the loop is stopped."""
        return _Recruiter(self._downstreams_stop)

    def submit(self, context: dict[int, Any] = {}):
        if self._iterator is None:
            start = self.read_context(context, "start")[0]
            stop = self.read_context(context, "stop")[0]
            step = self.read_context(context, "step")[0]
            self._iterator = iter(range(start, stop, step))
        try:
            current = next(self._iterator)
        except StopIteration:
            self._iterator = None
            return TaskItem(
                recruit=self._downstreams_stop,
                control=FlowCtrl.NONE
            )

        self.write(context, (current,))
        return TaskItem(
            recruit=self.downstreams,
            control=FlowCtrl.REPEAT
        )


class Break(Node):
    """Break the repeat loop."""
    def submit(self, context: dict[int, Any] = {}):
        return TaskItem(control=FlowCtrl.BREAK)


class Join(_Recruiter, Node):
    """Block the execution until all receivers are triggered."""
    def __init__(self, num: int = 2):
        _Recruiter.__init__(self)
        self.receivers = tuple(Join.Receiver(self, i) for i in range(num))
        self.flags = [False] * num

    def submit(self, context: dict[int, Any] = {}) -> TaskItem:
        if all(self.flags):
            self.flags = [False] * len(self.receivers)
            return TaskItem(recruit=self.downstreams)
        else:
            return TaskItem()


    class Receiver(Node):
        def __init__(self, parent: "Join", index: int):
            self.parent = parent
            self.index = index

        def submit(self, context: dict[int, Any] = {}):
            self.parent.flags[self.index] = True
            return TaskItem(recruit={self.parent,})


class Group(_Recruiter, _ContextReader, Node):
    """Node group that runs a internal graph."""
    def __init__(self, graph, values: dict[str, Any]):
        self._graph = graph
        _ContextReader.__init__(self, **values)
        _Recruiter.__init__(self)

    def submit(self, context: dict[int, Any] = {}) -> TaskItem:
        param_set = set(self._connects.keys()) & set(self._values.keys())
        kwargs: dict[str, Any] = {}

        for param in param_set:
            kwargs[param] = self.read_context(context, param)[0]

        return TaskItem(
            target=self._graph.forward,
            kwargs=kwargs,
            recruit=self.downstreams
        )
