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
"""Port identifier."""


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
    """Task item for a node."""
    target: Callable | None = None
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    recruit: Sequence[Node] | None = None
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

    Node is a computational unit that can be connected to other nodes.
    It supports method `submit` which receives a context of the environment,
    and returns an execution task back to the task queue.
    """
    def submit(self, context: dict[int, Any]) -> TaskItem:
        """Get a task to be submitted to the task queue.

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
    __call__ is available for any subscription to others.
    """
    _values: dict[str, Any]
    _connects: dict[str, PortId]

    def __init__(self, **kwargs: PortId | Any):
        self._values = {}
        self._connects = {}
        self(**kwargs)

    def __call__(self, **kwargs: PortId | Node | Any):
        for name, value in kwargs.items():
            if isinstance(value, PortId):
                self._connects[name] = value
            elif isinstance(value, Node):
                self._connects[name] = PortId(id(value), None)
            else:
                self._values[name] = value

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


class _RouterMixin:
    """Supports routing downstream nodes.

    Introduce `_downstreams` list for downstream nodes.
    __rshift__ is available for routing downstream nodes.
    """
    _downstreams: list[Node]

    def __init__(self, downstream: list[Node], /):
        self._downstreams = downstream

    def __rshift__[N: Node](self, other: N) -> N:
        self._downstreams.append(other)
        return other

    @property
    def downstreams(self) -> tuple[Node, ...]:
        return tuple(self._downstreams)


class Execute(_RouterMixin, _ContextReader, Node):
    """Computational Node object."""
    _target: Callable

    def __init__(self, target: Callable, /):
        _ContextReader.__init__(self)
        _RouterMixin.__init__(self, [])
        sig = inspect.signature(self._target)

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
    downstreams_true: list[Node]
    downstreams_false: list[Node]

    def __init__(self, condition: bool | PortId = False, /):
        _ContextReader.__init__(self, condition=condition)
        self.downstreams_false = []
        self.downstreams_true = []

    @property
    def true(self):
        """Execute downstream nodes when condition is True."""
        return _RouterMixin(self.downstreams_true)

    @property
    def false(self):
        """Execute downstream nodes when condition is False."""
        return _RouterMixin(self.downstreams_false)

    def submit(self, context: dict[int, Any] = {}):
        val, status = self.read_context(context, "condition")

        if bool(val) and status:
            return TaskItem(recruit=self.downstreams_true)
        else:
            return TaskItem(recruit=self.downstreams_false)


class Repeat(_RouterMixin, _ContextReader, Node):
    """Repeat the execution for multiple times."""
    @overload
    def __init__(self, stop: int | PortId = 1, /): ...
    @overload
    def __init__(self, start: int | PortId, stop: int | PortId, step: int | PortId = 1, /): ...
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
        _RouterMixin.__init__(self, [])
        self.iterator = None
        self.downstreams_stop = []

    @property
    def stop(self):
        """Execute downstream nodes when the loop is stopped."""
        return _RouterMixin(self.downstreams_stop)

    def submit(self, context: dict[int, Any] = {}):
        if self.iterator is None:
            start = self.read_context(context, "start")[0]
            stop = self.read_context(context, "stop")[0]
            step = self.read_context(context, "step")[0]
            self.iterator = iter(range(start, stop, step))
        try:
            current = next(self.iterator)
        except StopIteration:
            self.iterator = None
            return TaskItem(
                recruit=self.downstreams_stop,
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


class Join(_RouterMixin, Node):
    """Block the execution until all receivers are triggered."""
    def __init__(self, num: int = 2):
        super().__init__([])
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
            return TaskItem(recruit=[self.parent])


class Group(_RouterMixin, _ContextReader, Node):
    """Node group that runs a internal graph."""
    def __init__(self, graph, values: dict[str, Any]):
        self._graph = graph
        _ContextReader.__init__(self, **values)
        _RouterMixin.__init__(self, [])

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
