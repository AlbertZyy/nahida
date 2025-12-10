
__all__ = [
    "PortId",
    "NodeExecFeedback",
    "InputDataNotFoundError",
    "InputMissingError",
    "NodeExceptionError",
    "OutputLengthMismatchError",
    "DataAlreadyExistError",
    "Node",
    "Compute",
    "Branch",
    "Repeat"
]

import inspect
from inspect import _ParameterKind as PPK
from typing import Any, overload
from abc import abstractmethod, ABCMeta
from collections.abc import Callable, Iterable
from collections import namedtuple
from dataclasses import dataclass, field


PortId = namedtuple("PortId", ["node_id", "index"])
"""Port identifier."""


@dataclass(slots=True, frozen=True)
class NodeExecFeedback[N]:
    recruit: list[N] = field(default_factory=list)
    deactivate: bool = True


class InputDataNotFoundError(Exception):
    """Raised when connected input data cannot be found in the context."""


class InputMissingError(Exception):
    """Raised when any required input is missing."""


class NodeExceptionError(Exception):
    """Raised when an error occurs in the node's exception handling."""


class OutputLengthMismatchError(Exception):
    """Raised when number of outputs does not match the number of function
    return values."""


class DataAlreadyExistError(Exception):
    """Raised when output data already exist in the context."""


class Node(metaclass=ABCMeta):
    """Abstract base class for all nodes.

    Node is a computational unit that can be connected to other nodes.
    It supports method `run` which receives a context of the environment,
    and returns a execution report back to the task queue.

    ```
    def run(
        self,
        context: dict[PortId, Any]
    ) -> NodeExecFeedback: ...
    ```"""
    @abstractmethod
    def run(
        self,
        context: dict[PortId, Any]
    ) -> NodeExecFeedback["Node"]: ...


def get_inputs(func: Callable) -> Iterable[tuple[str, Any, bool]]:
    """Return the node's input names and defaults."""
    sig = inspect.signature(func)

    for name, param in sig.parameters.items():
        if param.kind in (PPK.POSITIONAL_OR_KEYWORD, PPK.KEYWORD_ONLY):
            has_default = param.default is not PPK.empty
            yield name, param.default, has_default
        if param.kind in (PPK.POSITIONAL_ONLY, PPK.VAR_POSITIONAL):
            raise TypeError("Positional-only parameters are not supported")


class ContextOps:
    values: dict[str, Any]
    connects: dict[str, PortId]

    def __init__(self):
        self.values = {}
        self.connects = {}

    def __call__(self, **kwargs: PortId | Any):
        for name, value in kwargs.items():
            if isinstance(value, PortId):
                self.connects[name] = value
            else:
                self.values[name] = value

    def read_value(self, context: dict[PortId, Any], name: str) -> tuple[Any, bool]:
        """Get value for an input from the context."""
        if name in self.connects:
            pack_id = self.connects[name]
            if pack_id in context:
                return context[pack_id], True

        if name in self.values:
            return self.values[name], True

        return None, False

    def write_value(self, context: dict[PortId, Any], values: tuple) -> None:
        """Put values into the context."""
        for i, val in enumerate(values):
            pack_id = PortId(id(self), i)
            context[pack_id] = val


class _ConnectableMixin:
    outputs: list[str]

    def __init__(self, outputs: list[str]):
        self.outputs = outputs

    def __len__(self) -> int:
        return len(self.outputs)

    def __getitem__(self, key: str | int) -> PortId:
        if isinstance(key, str):
            try:
                index = self.outputs.index(key)
            except ValueError as e:
                raise ValueError(f"No output named {key!r} in {self!r}") from e
        elif isinstance(key, int):
            if key >= len(self.outputs):
                raise IndexError(
                    f"Output index {key} out of range for {self!r}"
                )
            index = key
        else:
            raise TypeError(f"Invalid key type {type(key)}")

        return (id(self), index)


class _RouterMixin:
    downstreams: list[Node]

    def __init__(self, downstream: list[Node]):
        self.downstreams = downstream

    def __rshift__(self, other: Node) -> Node:
        self.downstreams.append(other)
        return other


class Compute(_RouterMixin, _ConnectableMixin, ContextOps):
    """Computational Node object."""
    _target: Callable

    def __init__(self, target: Callable, /, outputs: list[str]):
        super(_ConnectableMixin, self).__init__()
        super(_RouterMixin, self).__init__(outputs)
        super(Compute, self).__init__([])
        sig = inspect.signature(self._target)

        for param in sig.parameters.values():
            if param.kind in (PPK.POSITIONAL_ONLY, PPK.VAR_POSITIONAL):
                raise TypeError("Positional-only parameters are not supported")

        self._target = target

    def __repr__(self) -> str:
        return f"<Node {self._target.__name__} at {hex(id(self))}>"

    def run(self, context: dict[PortId, Any] = {}):
        """Execute the node"""
        input_kwargs = {}

        for param, _, has_default in get_inputs(self._target):
            val, status = self.read_value(context, param)
            if status:
                input_kwargs[param] = val
                continue
            if has_default:
                continue
            raise InputMissingError(f"No value for input {param!r} in {self!r}")

        try:
            result = self._target(**input_kwargs)
        except Exception as e:
            raise NodeExceptionError(
                f"Function in {self!r} raised an exception"
            ) from e

        if not isinstance(result, tuple):
            result = (result,)

        if len(result) != len(self.outputs):
            raise OutputLengthMismatchError(
                f"Function in {self!r} returned {len(result)} "
                f"values, but expected {len(self.outputs)}"
            )

        self.write_value(context, result)

        return NodeExecFeedback(
            recruit=self.downstreams,
            deactivate=True
        )

Node.register(Compute)


class Branch(_ConnectableMixin, ContextOps):
    downstreams_true: list[Node]
    downstreams_false: list[Node]

    def __init__(self, condition: bool | PortId = False, /):
        super(_ConnectableMixin, self).__init__()
        super(Branch, self).__init__([])
        self(condition=condition)
        self.downstreams_false = []
        self.downstreams_true = []

    @property
    def true(self):
        return _RouterMixin(self.downstreams_true)

    @property
    def false(self):
        return _RouterMixin(self.downstreams_false)

    def run(self, context: dict[PortId, Any] = {}):
        val, status = self.read_value(context, "condition")

        if bool(val) and status:
            return NodeExecFeedback(
                recruit=self.downstreams_true,
                deactivate=True
            )
        else:
            return NodeExecFeedback(
                recruit=self.downstreams_false,
                deactivate=True
            )

Node.register(Branch)


class Repeat(_RouterMixin, _ConnectableMixin, ContextOps):
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

        super(_ConnectableMixin, self).__init__()
        super(_RouterMixin, self).__init__(["current"])
        super(Repeat, self).__init__([])
        self(start=start, stop=stop, step=step)
        self.iterator = None

    def run(self, context: dict[PortId, Any] = {}):
        if self.iterator is None:
            start = self.read_value(context, "start")[0]
            stop = self.read_value(context, "stop")[0]
            step = self.read_value(context, "step")[0]
            self.iterator = iter(range(start, stop, step))
            current = next(self.iterator)
        # NOTE: Advance the iterator by one step so that when the next step is
        # about to trigger StopIteration, the loop exits immediately.
        # This avoids triggering StopIteration at the current step and not
        # recruiting any downstream nodes.
        try:
            next_val = next(self.iterator)
        except StopIteration:
            self.iterator = None

        self.write_value(context, (current,))
        current = next_val
        return NodeExecFeedback(
            recruit=self.downstreams,
            deactivate=self.iterator is None
        )

Node.register(Repeat)
