from typing import Optional, Any, overload

__all__ = [
    "OutputSlot",
    "InputSlot"
]


class Slot():
    _name : str
    __slots__ = ("_name")

    def __hash__(self): return id(self)

    @property
    def name(self): return self._name

    def rename(self, name: str):
        self._name = name


class OutputSlot(Slot):
    _node : Any
    _ref : int
    __slots__ = ("_node", "_ref")

    def __init__(self, name: str, node: Any):
        self._name = name
        self._node = node
        self._ref = 0

    @property
    def node(self): return self._node
    @property
    def ref(self): return self._ref

    def __repr__(self): return f"{self.node}->{self.name}"


class InputSlot(Slot):
    _source : Optional[OutputSlot]
    _empty : object
    _default : Any
    __slots__ = ("_source", "_empty", "_default")

    @overload
    def __init__(self, name: str): ...
    @overload
    def __init__(self, name: str, *, default: Any): ...
    def __init__(self, name: str, **kwargs):
        self._name = name
        self._source = None
        self._empty = object()

        if "default" in kwargs:
            self._default = kwargs["default"]
        else:
            self._default = self._empty

    @property
    def source(self): return self._source
    @property
    def default(self): return self._default

    def has_default(self):
        return self._default is not self._empty

    def is_connected(self):
        return self._source is not None

    def connect(self, source: OutputSlot) -> None:
        source._ref += 1
        self._source = source

    def disconnect(self) -> None:
        source = self._source
        self._source = None
        source._ref -= 1
