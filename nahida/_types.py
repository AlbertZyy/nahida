from typing import (
    Any, Type, Optional,
    Dict, Mapping,
    Protocol
)
from dataclasses import dataclass, field
from enum import IntEnum, auto


class SlotStatus(IntEnum):
    ACTIVE = auto()
    BLOCKED = auto()
    DISABLED = auto()


class OutputSlot(Protocol):
    ## Status
    def get_status(self) -> SlotStatus: ...
    def set_status(self, status: SlotStatus) -> None: ...
    ## Connection
    def get_source(self) -> "Node": ...


class InputSlot(Protocol):
    ## Status
    def get_status(self) -> SlotStatus: ...
    def set_status(self, status: SlotStatus) -> None: ...
    ## Default value
    def has_value(self) -> bool: ...
    def get_value(self) -> Any: ...
    def set_value(self, value: Any, /) -> None: ...
    def del_value(self) -> None: ...
    ## Connection
    def is_connected(self) -> bool: ...
    def get_source(self) -> OutputSlot | None: ...
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


@dataclass(frozen=True, slots=True)
class NodeExceptionData():
    node : Node
    timestamp : float = 0.
    type : Type[Exception] = Exception
    message : Optional[str] = None
    inputs : Dict[str, Any] = field(default_factory=dict)


class NodeIOError(Exception):
    """Inappropriate input or output of nodes."""
    pass


class NodeTopologyError(Exception):
    """Inappropriate connection."""
    pass


class GraphStatusError(Exception):
    """Inappropriate status of graph."""
    pass


class GraphStatus(IntEnum):
    READY = auto()
    RUN = auto()
    DEBUG = auto()
    STOP = auto()
