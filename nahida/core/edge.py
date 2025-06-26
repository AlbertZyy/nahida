from collections.abc import Mapping

from ._types import Node, InputSlot, NodeTopologyError

__all__ = ["connect", "OutputAddress", "connect_from_address"]


def connect(
        source_node: Node,
        source_slot: str,
        target_slot: InputSlot
) -> None:
    if target_slot.is_connected():
        raise NodeTopologyError("Input slot is already connected")

    target_slot.source_node = source_node
    target_slot.source_slot = source_slot


class OutputAddress:
    __slots__ = ("_node", "_slot")

    def __init__(self, node: Node, slot: str | None):
        self._node = node
        self._slot = slot

    def __getattr__(self, name: str):
        self._slot = name
        return self

    def _take_the_first_port(self) -> None:
        for name in self._node.output_slots.keys():
            self._port = name
            break

    def get_addr(self) -> tuple[Node, str]:
        if self._port is None:
            self._take_the_first_port()
        elif self._port not in self._node.output_slots:
            raise NodeTopologyError(f"Output slot {self._port} does not exist")

        return self._node, self._port


def connect_from_address(
        input_slots: Mapping[str, InputSlot],
        addr_kwds: Mapping[str, OutputAddress]
) -> None:
    for name, addr in addr_kwds.items():
        if name in input_slots:
            source_node, source_slot = addr.get_addr()
            try:
                connect(source_node, source_slot, input_slots[name])
            except NodeTopologyError:
                raise NodeTopologyError(f"Input slot {name} is already connected")
        else:
            raise NodeTopologyError(f"Input slot {name} does not exist")
