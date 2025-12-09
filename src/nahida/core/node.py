
__all__ = [
    "PortId",
    "NodeExcutionReport",
    "InputDataNotFoundError",
    "InputMissingError",
    "NodeExceptionError",
    "OutputLengthMismatchError",
    "DataAlreadyExistError",
    "Node",
    "ComputeNode",
    "Branch",
    "FiniteLoop"
]

import inspect
from inspect import _ParameterKind as PPK
from typing import Any
from abc import abstractmethod, ABCMeta
from collections.abc import Callable, Iterable
from dataclasses import dataclass


type PortId = tuple[int, int]
"""Port identifier."""


@dataclass(slots=True, frozen=True)
class NodeExcutionReport[N]:
    recruit: list[N]
    deactivate: bool


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
    ) -> NodeExcutionReport: ...
    ```"""
    @abstractmethod
    def run(
        self,
        context: dict[PortId, Any]
    ) -> NodeExcutionReport["Node"]: ...


def get_inputs(func: Callable) -> Iterable[tuple[str, Any, bool]]:
    """Return the node's input names and defaults."""
    sig = inspect.signature(func)

    for name, param in sig.parameters.items():
        if param.kind in (PPK.POSITIONAL_ONLY, PPK.VAR_POSITIONAL):
            raise TypeError("Positional-only parameters are not supported")
        if param.kind in (PPK.POSITIONAL_OR_KEYWORD, PPK.KEYWORD_ONLY):
            has_default = param.default is not PPK.empty
            yield name, param.default, has_default


class ContextOps:
    values: dict[str, Any]
    connects: dict[str, PortId]
    outputs: list[str]

    def __init__(self):
        self.values = {}
        self.connects = {}
        self.outputs = []

    def pull(self, context: dict[PortId, Any], name: str) -> tuple[Any, bool]:
        """Get value for an input from the context."""
        if name in self.connects:
            pack_id = self.connects[name]
            if pack_id not in context:
                raise InputDataNotFoundError(
                    f"Value for input {name!r} of {self!r} not found "
                    "in the context. Make sure the compute order is correct."
                )
            return context[pack_id], True
        elif name in self.values:
            return self.values[name], True
        else:
            return None, False

    def push(self, context: dict[PortId, Any], values: tuple) -> None:
        """Put values into the context."""
        for i, (name, val) in enumerate(zip(self.outputs, values)):
            pack_id = (id(self), i)
            if pack_id in context:
                raise DataAlreadyExistError(
                    f"Output {name!r} of {self!r} already exists in the context"
                )
            context[pack_id] = val


class ComputeNode(ContextOps):
    """Computational Node object."""
    _target: Callable
    downstreams: list[Node]

    def __init__(self, target: Callable, /):
        super().__init__()
        sig = inspect.signature(self._target)

        for param in sig.parameters.values():
            if param.kind in (PPK.POSITIONAL_ONLY, PPK.VAR_POSITIONAL):
                raise TypeError("Positional-only parameters are not supported")

        self._target = target
        self.downstreams = []

    def __repr__(self) -> str:
        return f"<Node {self._target.__name__} at {hex(id(self))}>"

    def __getitem__(self, name: str) -> PortId:
        try:
            return (id(self), self.outputs.index(name))
        except ValueError as e:
            raise ValueError(f"No output named {name} in {self!r}") from e

    def __call__(self, **kwargs: PortId):
        self.connects.update(kwargs)

    def run(self, context: dict[PortId, Any] = {}):
        """Execute the node"""
        input_kwargs = {}

        for param, _, has_default in get_inputs(self._target):
            val, status = self.pull(context, param)
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

        self.push(context, result)

        return NodeExcutionReport(
            recruit=self.downstreams,
            deactivate=True
        )

Node.register(ComputeNode)


class Branch:
    condition_port: PortId | None
    value: bool
    downstreams_true: list[Node]
    downstreams_false: list[Node]

    def __init__(self, condition: PortId | bool = False):
        if isinstance(condition, tuple):
            self.condition_port = condition
            self.value = False
        else:
            self.condition_port = None
            self.value = bool(condition)

        self.downstreams_false = []
        self.downstreams_true = []

    @property
    def true(self): ...

    @property
    def false(self): ...

    def run(self, context: dict[PortId, Any] = {}):
        if self.condition_port is None:
            val = self.value
        else:
            if self.condition_port not in context:
                raise InputDataNotFoundError

            val = bool(context[self.condition])

        if val:
            return NodeExcutionReport(
                recruit=self.downstreams_true,
                deactivate=True
            )
        else:
            return NodeExcutionReport(
                recruit=self.downstreams_false,
                deactivate=True
            )

Node.register(Branch)


class FiniteLoop:
    iters: int
    start: int
    downstreams: list[Node]

    def __init__(self, iters: PortId | int = 0, start: int = 0):
        self.iters = iters
        self.downstreams = []

    def run(self, context: dict[PortId, Any] = {}):
        if self.iters > 1:
            return NodeExcutionReport(
                recruit=self.downstreams,
                deactivate=False
            )
        else:
            return NodeExcutionReport(
                recruit=self.downstreams,
                deactivate=True
            )

Node.register(FiniteLoop)
