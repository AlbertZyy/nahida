from typing import Optional, Any, overload

from .._types import SlotStatus

__all__ = [
    "OutputPort",
    "InputPort"
]


class Slot():
    __slots__ = ("status", "_status_change_hook")
    status : SlotStatus

    def __init__(self):
        self.status = SlotStatus.ACTIVE
        self._status_change_hook = None

    def get_status(self):
        return self.status

    def set_status(self, value: SlotStatus):
        self.status = value


class OutputSlot(Slot):
    __slots__ = ("source",)
    source : Any

    def __init__(self, source: Any, /):
        super().__init__()
        self.source = source

    def get_source(self):
        return self.source


class InputSlot(Slot):
    __slots__ = ("_empty", "source", "value")
    _empty : object
    source : Optional[OutputSlot]
    value : Any

    @overload
    def __init__(self): ...
    @overload
    def __init__(self, *, value: Any): ...
    def __init__(self, **kwargs):
        super().__init__()
        self._empty = object()
        self.source = None
        self.value = kwargs.get("value", self._empty)

    def has_value(self):
        return self.value is not self._empty

    def get_value(self):
        return self.value

    def set_value(self, value: Any):
        self.value = value

    def del_value(self):
        self.value = self._empty

    def is_connected(self):
        return self.source is not None

    def get_source(self):
        return self.source

    def connect(self, source: OutputSlot) -> None:
        self.source = source

    def disconnect(self) -> None:
        self.source = None
