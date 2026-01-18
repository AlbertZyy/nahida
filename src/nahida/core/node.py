# nahida/core/node.py

from __future__ import annotations

__all__ = [
    "FlowCtrl",
    "TaskItem",
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
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum

from . import errors as _err
from . import expr as _expr


type Expr = _expr.Expr


class FlowCtrl(StrEnum):
    """Workflow control instruction after execution."""

    NONE = "none"
    """Do nothing and be removed from the tasks."""

    ENTER = "enter"
    """Require a new scope for downstreams."""

    EXIT = "exit"
    """Require cancel the current scope."""


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


class Node(_expr.Expr):
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
    def __init__(self, *, uid: int | None = None) -> None:
        from uuid import uuid4
        self._uid = int(uuid4()) if uid is None else uid

    @property
    def uid(self) -> int:
        return self._uid

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
        """Put output values into the context.

        Args:
            context (dict[int, Any]): The context of the environment.
            values (Any): The values to be put into the context.
        """
        context[self.uid] = values

    def eval(self, context: dict[int, Any], /) -> Any:
        """Get output values from the context.

        Raises *DataNotFoundError* if uid is not exsist."""
        try:
            return context[self.uid]
        except KeyError as e:
            raise _err.DataNotFoundError(self) from e

    def __repr__(self) -> str:
        type_name = self.__class__.__name__
        return "<{} node at {}>".format(type_name, hex(id(self)))


def get_inputs(func: Callable) -> Iterable[tuple[str, Any, bool]]:
    """Return the node's input names and defaults."""
    sig = inspect.signature(func)

    for name, param in sig.parameters.items():
        if param.kind in (PPK.POSITIONAL_OR_KEYWORD, PPK.KEYWORD_ONLY):
            has_default = param.default is not param.empty
            yield name, param.default, has_default
        if param.kind in (PPK.POSITIONAL_ONLY, PPK.VAR_POSITIONAL):
            raise TypeError("Positional-only parameters are not supported")


class _NamedObj:
    """Support unique names for __repr__."""
    _uname: str | None = None

    def __repr__(self) -> str:
        type_name = self.__class__.__name__

        if self._uname:
            return "{}({})".format(type_name, self._uname)

        return super().__repr__()

    @property
    def __name__(self):
        if self._uname:
            return self._uname

        return super().__repr__()

    def set_name(self, name: str | None, /):
        self._uname = name


class _ContextReader:
    """Supports read_context in a node.

    Introduce `_values` and `_connects` dicts for defaults and subscriptions.
    """
    _attributes: dict[str, Expr]

    def __init__(self, **kwargs: Expr | Any):
        self._attributes = {}
        self.subs(**kwargs)

    def subs(self, **kwargs: Expr | Any) -> None:
        """Set subscriptions for attributes.

        An attribute may take values from other nodes or constants. Use pairs
        like `arg=node` or `arg=value` to define the souce of the attribute
        named `arg`. Use `arg=node[index]` for source nodes returning a
        tuple, list, dict or any other types that supports __getitem__.

        Args:
            **kwargs (PortId | Node | Any): The attribute-source pairs.
        """
        for name, value in kwargs.items():
            if _expr.is_expr(value):
                self._attributes[name] = value
            else:
                self._attributes[name] = _expr.constant(value)

    def unsubs(self, *names: str) -> None:
        """Remove subscriptions for attributes."""
        for name in names:
            self._attributes.pop(name, None)

    def read_context(self, context: dict[int, Any], name: str) -> tuple[Any, bool]:
        """Get value for an input from the context."""
        if name in self._attributes:
            try:
                return self._attributes[name].eval(context), True
            except Exception as e:
                raise _err.SubscribeError(self, name) from e

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
        self._downstreams.update(other)

    def unlink(self, *other: Node) -> None:
        """Remove downstream nodes. Do nothing for non-existent nodes.

        Args:
            *other (Node): The downstream nodes to be removed.
        """
        self._downstreams.difference_update(other)

    @property
    def downstreams(self) -> set[Node]:
        return self._downstreams


class Execute(_Recruiter, _ContextReader, _NamedObj, Node):
    """Computational Node object."""
    _target: Callable

    def __init__(self, target: Callable, /, *, uid: Any = None):
        Node.__init__(self, uid=uid)
        _ContextReader.__init__(self)
        _Recruiter.__init__(self)
        sig = inspect.signature(target)

        for param in sig.parameters.values():
            if param.kind in (PPK.POSITIONAL_ONLY, PPK.VAR_POSITIONAL):
                raise TypeError("Positional-only parameters are not supported")

        self._target = target

    def submit(self, context: dict[int, Any] = {}):
        input_kwargs = {}

        for param, _, has_default in get_inputs(self._target):
            val, status = self.read_context(context, param)
            if status:
                input_kwargs[param] = val
                continue
            if has_default:
                continue
            raise _err.ParamMissingError(self, param)

        return TaskItem(
            target=self._target,
            kwargs=input_kwargs,
            recruit=self.downstreams
        )


class Branch(_ContextReader, _NamedObj, Node):
    """Branch the execution based on a condition."""
    _downstreams_true: set[Node]
    _downstreams_false: set[Node]

    def __init__(self, condition: Expr | bool = False, /, *, uid: int | None = None) -> None:
        Node.__init__(self, uid=uid)
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


class Repeat(_ContextReader, _NamedObj, Node):
    """Repeat the execution for multiple times."""
    def __init__(self, iterable: Expr | Iterable[Any] | None = None, *, uid: int | None = None) -> None:
        Node.__init__(self, uid=uid)
        _ContextReader.__init__(self, iterable=iterable)
        self._downstreams_iter: set[Node] = set()
        self._downstreams_stop: set[Node] = set()
        self._iterator = None

    @property
    def iter(self):
        """Execute downstream nodes repeatedly."""
        return _Recruiter(self._downstreams_iter)

    @property
    def stop(self):
        """Execute downstream nodes when the loop is stopped."""
        return _Recruiter(self._downstreams_stop)

    def submit(self, context: dict[int, Any] = {}):
        if self._iterator is None:
            iterable = self.read_context(context, "iterable")[0]
            self._iterator = iter(iterable)
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
            recruit=self._downstreams_iter,
            control=FlowCtrl.ENTER
        )

    @overload
    @classmethod
    def from_range(cls, stop: int | Expr = 1, /, *, uid: Any = None) -> Repeat: ...
    @overload
    @classmethod
    def from_range(cls, start: int | Expr, stop: int | Expr, step: int | Expr = 1, /, *, uid: Any = None) -> Repeat: ...
    @classmethod
    def from_range(cls, *args, uid: Any = None) -> Repeat:
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
        range_expr = _expr.formula(
            "range(start, stop, step)",
            start=start, stop=stop, step=step
        )
        return cls(range_expr, uid=uid)


class Break(_NamedObj, Node):
    """Break the repeat loop."""
    def submit(self, context: dict[int, Any] = {}):
        return TaskItem(control=FlowCtrl.EXIT)


class Join(_Recruiter, _NamedObj, Node):
    """Block the execution until all receivers are triggered."""
    def __init__(self, num: int = 2, *, uid: Any = None):
        Node.__init__(self, uid=uid)
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


class Group(_Recruiter, _ContextReader, _NamedObj, Node):
    """Node group that runs a internal graph."""
    def __init__(self, graph, values: dict[str, Any], *, uid: Any = None):
        Node.__init__(self, uid=uid)
        _ContextReader.__init__(self, **values)
        _Recruiter.__init__(self)
        self._graph = graph

    def submit(self, context: dict[int, Any] = {}) -> TaskItem:
        param_set = set(self._attributes.keys())
        kwargs: dict[str, Any] = {}

        for param in param_set:
            kwargs[param] = self.read_context(context, param)[0]

        return TaskItem(
            target=self._graph.run,
            kwargs=kwargs,
            recruit=self.downstreams
        )
