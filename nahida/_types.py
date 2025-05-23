from typing import (
    Any, Type, Optional,
    Dict, Mapping,
    Protocol
)
from dataclasses import dataclass, field
from enum import IntEnum, auto


class OutputSlot(Protocol):
    @property
    def name(self) -> str: ...


class InputSlot(Protocol):
    @property
    def name(self) -> str: ...
    @property
    def source(self) -> OutputSlot | None: ...
    @property
    def default(self) -> Any: ...
    def has_default(self) -> bool: ...
    def is_connected(self) -> bool: ...
    def connect(self, source: OutputSlot, /) -> None: ...
    def disconnect(self) -> None: ...


class Node(Protocol):
    @property
    def input_slots(self) -> Mapping[str, InputSlot]: ...
    @property
    def output_slots(self) -> Mapping[str, OutputSlot]: ...
    @property
    def options(self) -> Mapping[str, Any]: ...
    def run(self, *args, **kwargs) -> Any: ...


@dataclass
class NodeExceptionData():
    node : Node
    timestamp : float = 0.
    type : Type[Exception] = Exception
    message : Optional[str] = None
    inputs : Dict[str, Any] = field(default_factory=dict)


class NodeIOError(Exception):
    """Inappropriate input or output of nodes."""
    pass


class GraphStatusError(Exception):
    """Inappropriate status of graph."""
    pass


class GraphStatus(IntEnum):
    READY = auto()
    RUN = auto()
    DEBUG = auto()
    STOP = auto()
