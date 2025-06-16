from typing import Dict, Tuple

from ._types import InputSlot, OutputSlot, Node, NodeTopologyError


class _ConnPort():
    def __init__(self, container: Dict, node: Node):
        self._node = node
        self._container = container
        self._port = []

    def __getattr__(self, name: str):
        self._port.append(name)
        return self

    def __getitem__(self, names: str | Tuple[str, ...]):
        if len(names) == 1:
            names = names[0].split(",")
            names = [n.strip() for n in names]
        self._port = list(names)
        return self

    def get_slot(self) -> Dict[str, InputSlot | OutputSlot]:
        if len(self._port) == 0:
            return self._container
        return {p: self._container[p] for p in self._port}


class _ConnPortIn(_ConnPort):
    def recv(self, other: "_ConnPortOut") -> None:
        self_slots = self.get_slot()
        other_slots = other.get_slot()

        for self_slot, (oname, other_slot) in zip(self_slots.values(), other_slots.items()):
            if isinstance(self_slot, InputSlot) and isinstance(other_slot, OutputSlot):
                self_slot.source_node = other._node
                self_slot.source_slot = oname
            else:
                raise NodeTopologyError(f"Input should receives data from an output.")

    __lshift__ = recv


class _ConnPortOut(_ConnPort):
    def sent(self, other: "_ConnPortIn"):
        self_slots = self.get_slot()
        other_slots = other.get_slot()

        for (sname, self_slot), other_slot in zip(self_slots.items(), other_slots.values()):
            if isinstance(self_slot, OutputSlot) and isinstance(other_slot, InputSlot):
                other_slot.source_node = self._node
                other_slot.source_slot = sname
            else:
                raise NodeTopologyError(f"Output should sent data to an input.")

        return self

    __rshift__ = sent