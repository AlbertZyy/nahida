from typing import (
    Any, Type, Optional,
    Tuple, List, Dict, Mapping,
    Protocol
)
from inspect import _ParameterKind
from dataclasses import dataclass, field
from enum import IntEnum, auto


class SlotStatus(IntEnum):
    ACTIVE = auto()
    BLOCKED = auto()
    DISABLED = auto()


@dataclass(slots=True)
class DataBox():
    has_data: bool = False
    data: Any = None


@dataclass(slots=True)
class InputSlot():
    status: SlotStatus = SlotStatus.ACTIVE
    source_node: Optional["Node"] = None
    source_slot: Optional[str] = None
    has_default: bool = False
    default: Any = None
    param_kind: _ParameterKind = _ParameterKind.POSITIONAL_OR_KEYWORD
    databox: Optional[DataBox] = None


@dataclass(slots=True)
class OutputSlot():
    status: SlotStatus = SlotStatus.ACTIVE
    databox: Optional[DataBox] = None


class Node(Protocol):
    @property
    def input_slots(self) -> Mapping[str, InputSlot]: ...
    @property
    def output_slots(self) -> Mapping[str, OutputSlot]: ...
    @property
    def is_variable(self) -> bool: ...
    def run(self, *args, **kwargs) -> Any: ...
    def execute(self) -> "NodeExceptionData | None": ...
    def dump_key(self, slot: str) -> Any: ...


@dataclass(frozen=True, slots=True)
class NodeExceptionData():
    node : Node
    timestamp : float = 0.
    type : Type[Exception] = Exception
    message : Optional[str] = None
    positional_inputs : List[Any] = field(default_factory=list)
    keyword_inputs : Dict[str, Any] = field(default_factory=dict)


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
