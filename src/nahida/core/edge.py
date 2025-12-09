from collections.abc import Mapping

from ._types import Node, TransSlot, NodeTopologyError

__all__ = ["connect", "AddrHandler", "connect_from_address"]


class AddrHandler:
    __slots__ = ("_node", "_slot", "_output")

    def __init__(self, node: Node, slot: str | None, output=True):
        self._node = node
        self._slot = slot
        self._output = output

    def __getattr__(self, name: str):
        self._slot = name
        return self

    def _take_the_first_port(self, slot_dict) -> None:
        for name in slot_dict.keys():
            self._slot = name
            break
        else:
            msg = "output" if self._output else "input"
            raise NodeTopologyError(f"No {msg} slot found in node {self._node}")

    def get_addr(self) -> tuple[Node, str]:
        slot_dict = self._node.output_slots if self._output else self._node.input_slots

        if self._slot is None:
            self._take_the_first_port(slot_dict)
        elif self._slot not in slot_dict:
            msg = "Output" if self._output else "Input"
            raise NodeTopologyError(f"{msg} slot '{self._slot}' does not exist "
                                    f"in node {self._node}")

        return self._node, self._slot


def connect_from_address(
        slots: Mapping[str, TransSlot],
        addr_kwds: Mapping[str, AddrHandler | tuple[AddrHandler]],
) -> None:
    for name, addrs in addr_kwds.items():
        if not isinstance(addrs, (tuple, list)): addrs = (addrs,)
        if name in slots:
            in_slot = slots[name]
            for addr in addrs:
                source_node, source_slot = addr.get_addr()
                in_slot.connect(source_node, source_slot)
        else:
            raise NodeTopologyError(f"Input slot {name} does not exist")
