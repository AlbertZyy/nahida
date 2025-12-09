from typing import Any, Protocol, runtime_checkable, NamedTuple
from collections import namedtuple
from collections.abc import Mapping
from inspect import _ParameterKind
from dataclasses import dataclass, field
from enum import IntEnum, auto


class ParamPassingKind(IntEnum):
    POSITIONAL     = 0
    KEYWORD        = 1
    VAR_POSITIONAL = 2
    VAR_KEYWORD    = 3


class SlotStatus(IntEnum):
    ACTIVE = auto()
    BLOCKED = auto()
    DISABLED = auto()


class SourceAddr(NamedTuple):
    """A named tuple containing the source node and slot name."""
    node: "Node"
    slot: str


@dataclass(slots=True)
class Slot():
    """Slot with status"""
    status: SlotStatus = SlotStatus.ACTIVE

    def __hash__(self) -> int:
        return id(self)

    def is_active(self) -> bool:
        return self.status == SlotStatus.ACTIVE

    def is_blocked(self) -> bool:
        return self.status == SlotStatus.BLOCKED

    def is_disabled(self) -> bool:
        return self.status == SlotStatus.DISABLED


@dataclass(slots=True)
class TransSlot(Slot):
    has_default : bool             = False
    default     : Any              = None
    source_list : list[SourceAddr] = field(default_factory=list, init=False, compare=False)

    def is_connected(self) -> bool:
        return len(self.source_list) > 0

    def connect(self, source: "Node", slot: str):
        if not self.is_connected():
            self.source_list.append(SourceAddr(source, slot))
        else:
            raise NodeTopologyError("multiple connections to non-variable input")


@dataclass(slots=True)
class InputSlot(TransSlot):
    variable    : bool             = False
    param_name  : str | None       = None
    param_kind  : int              = 1

    def __post_init__(self) -> None:
        if self.param_kind == ParamPassingKind.VAR_KEYWORD and self.variable:
            raise TypeError("data from variable slots are not allowed to be "
                            "variable keyword arguments")

    def connect(self, source: "Node", slot: str):
        if self.variable or not self.is_connected():
            self.source_list.append(SourceAddr(source, slot))
        else:
            raise NodeTopologyError("multiple connections to non-variable input")


OutputSlot = Slot


@runtime_checkable
class Node(Protocol):
    """Protocol for computational nodes"""
    @property
    def input_slots(self) -> Mapping[str, InputSlot]: ...
    @property
    def output_slots(self) -> Mapping[str, OutputSlot]: ...
    def run(self, *args, **kwargs) -> Any: ...


class NodeGroup(Protocol):
    """Protocol for node groups"""
    @property
    def input_slots(self) -> Mapping[str, InputSlot]: ...
    @property
    def output_slots(self) -> Mapping[str, TransSlot]: ...
    def run(self, *args, **kwargs) -> Any: ...


@dataclass(frozen=True, slots=True)
class NodeExceptionData():
    node : Node
    timestamp : float = 0.
    errtype : type[Exception] = Exception
    message : str | None = None
    positional_inputs : list[Any] = field(default_factory=list)
    keyword_inputs : dict[str, Any] = field(default_factory=dict)


class NodeIOError(Exception):
    """Inappropriate input or output of nodes."""
    pass


class NodeTopologyError(Exception):
    """Inappropriate connection."""
    pass


class GraphStatusError(Exception):
    """Inappropriate status of graph."""
    pass
