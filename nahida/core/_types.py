from typing import Any, Protocol, runtime_checkable
from collections.abc import Mapping
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

    def put(self, data: Any) -> None:
        self.has_data = True
        self.data = data

    def get(self) -> Any:
        if not self.has_data:
            raise ValueError("DataBox is empty.")
        return self.data


@dataclass(slots=True)
class Slot():
    status: SlotStatus = SlotStatus.ACTIVE

    def is_active(self) -> bool:
        return self.status == SlotStatus.ACTIVE

    def is_blocked(self) -> bool:
        return self.status == SlotStatus.BLOCKED

    def is_disabled(self) -> bool:
        return self.status == SlotStatus.DISABLED


@dataclass(slots=True)
class _InputSlot(Slot):
    param_name: str | None = None
    has_default: bool = False
    default: Any = None
    param_kind: _ParameterKind = _ParameterKind.POSITIONAL_OR_KEYWORD


@dataclass(slots=True)
class InputSlot(_InputSlot):
    source_node: "Node | None" = field(default=None, init=False, compare=False)
    source_slot: str | None = field(default=None, init=False, compare=False)

    def is_connected(self) -> bool:
        return self.is_active() \
            and self.source_node is not None \
            and self.source_slot is not None


@dataclass(slots=True)
class VariableSlot(_InputSlot):
    source_node: list["Node"] = field(default_factory=list, init=False, compare=False)
    source_slot: list[str] = field(default_factory=list, init=False, compare=False)

    def is_connected(self) -> bool:
        return self.is_active() \
            and self.source_node \
            and self.source_slot

    def __post_init__(self) -> None:
        if self.param_kind == _ParameterKind.VAR_KEYWORD:
            raise TypeError("data from variable slots are not allowed to be "
                            "variable keyword arguments")

    def __len__(self) -> int:
        return len(self.source_node)


@dataclass(slots=True)
class OutputSlot(Slot):
    pass


@runtime_checkable
class Node(Protocol):
    @property
    def input_slots(self) -> Mapping[str, InputSlot]: ...
    @property
    def output_slots(self) -> Mapping[str, OutputSlot]: ...
    @property
    def is_variable(self) -> bool: ...
    def run(self, *args, **kwargs) -> Any: ...
    def dump_key(self, slot: str) -> Any: ...


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


class GraphStatus(IntEnum):
    READY = auto()
    RUN = auto()
    DEBUG = auto()
    STOP = auto()
