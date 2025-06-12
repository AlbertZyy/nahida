from typing import Dict

from .core.connection import InputSlot, OutputSlot


class _ConnPort():
    def __init__(self, container: Dict):
        self._container = container
        self._port = []

    def __getattr__(self, name: str):
        self._port.append(name)
        return self

    def __getitem__(self, *names: str):
        if len(names) == 1:
            names = names[0].split(",")
            names = [n.strip() for n in names]
        self._port = list(names)
        return self

    def get_slot(self):
        if len(self._port) == 0:
            return list(self._container.values())
        return [self._container[p] for p in self._port]


class _ConnPortIn(_ConnPort):
    def recv(self, other: "_ConnPortOut") -> None:
        self_slots = self.get_slot()
        other_slots = other.get_slot()

        for self_slot, other_slot in zip(self_slots, other_slots):
            if isinstance(self_slot, InputSlot) and isinstance(other_slot, OutputSlot):
                self_slot.connect(other_slot)
            else:
                raise RuntimeError(f"Input should receives data from an output.")

    __lshift__ = recv


class _ConnPortOut(_ConnPort):
    def sent(self, other: "_ConnPortIn"):
        self_slots = self.get_slot()
        other_slots = other.get_slot()

        for self_slot, other_slot in zip(self_slots, other_slots):
            if isinstance(self_slot, OutputSlot) and isinstance(other_slot, InputSlot):
                other_slot.connect(self_slot)
            else:
                raise RuntimeError(f"Output should sent data to an input.")

        return self

    __rshift__ = sent