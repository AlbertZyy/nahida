from typing import Any, TypeVar
from collections.abc import Iterable
from dataclasses import dataclass, field

from ._types import NodeIOError, Node, NodeGroup, Slot, TransSlot
from ._types import ParamPassingKind as PPK

_T = TypeVar("_T")


@dataclass(eq=False, match_args=False, slots=True)
class DataBox:
    has_data: bool = field(default=False, init=False)
    data: Any = field(default=None, init=False)

    def put(self, data: Any) -> None:
        self.has_data = True
        self.data = data

    def get(self) -> Any:
        if not self.has_data:
            raise ValueError("DataBox is empty.")
        return self.data


class NahidaCtxOperator:
    @staticmethod
    def create_box(supply_ctx: dict[int, DataBox], slot: Slot):
        """Create databox for a slot in `supply_ctx`."""
        if slot.is_active():
            supply_ctx[id(slot)] = DataBox()

    @staticmethod
    def share_box(
        supply_ctx: dict[int, DataBox],
        demand_ctx: dict[int, list[DataBox]],
        sources: Iterable[tuple[Node, str]],
        slot: Slot
    ):
        """Share databoxes to a slot from its sources, from `supply_ctx` to `demand_ctx`."""
        if not slot.is_active():
            return

        for src_node, src_slot in sources:
            source_key = id(src_node.output_slots[src_slot])

            if source_key not in supply_ctx:
                raise NodeIOError("Input data box is not found.")

            data_box = supply_ctx[source_key]
            context_key = id(slot)

            if context_key not in demand_ctx:
                demand_ctx[context_key] = []

            demand_ctx[context_key].append(data_box)

    @staticmethod
    def receive_data(
        demand_ctx: dict[int, list[DataBox]],
        node: Node,
        positional: list,
        keyword: dict[str, Any]
    ) -> None:
        for name, input_slot in node.input_slots.items():
            if input_slot.is_disabled():
                continue

            if input_slot.is_connected():
                databox_list = demand_ctx.pop(id(input_slot), None)
                if databox_list is None:
                    raise NodeIOError(f"No databox found for the input '{repr(node)}.{name}', "
                                      "construct the databox sharing first.")
                if getattr(input_slot, "variable", False):
                    data = tuple(databox.get() for databox in databox_list)
                else:
                    assert len(databox_list) == 1
                    databox = databox_list[0]
                    if not databox.has_data:
                        raise NodeIOError(
                            f"The databox for input '{repr(node)}.{name}' is empty, "
                            f"make sure the calculation order is appropriate."
                        )
                    data = databox.get()
            elif input_slot.has_default:
                data = input_slot.default
            else:
                continue

            param_kind = input_slot.param_kind
            if param_kind == PPK.POSITIONAL:
                positional.append(data)
            elif param_kind == PPK.VAR_POSITIONAL:
                assert isinstance(data, tuple)
                positional.extend(data)
            elif param_kind == PPK.KEYWORD:
                param_name = input_slot.param_name
                if param_name is None:
                    raise NodeIOError("Parameter name is expected if the parameter "
                                      "kind is not POSITIONAL_ONLY or variable length.")
                keyword[param_name] = data
            elif param_kind == PPK.VAR_KEYWORD:
                assert isinstance(data, dict)
                keyword.update(data)
            else:
                raise RuntimeError("Inappropriate parameter kind")

    @staticmethod
    def send_data(
        supply_ctx: dict[int, DataBox],
        node: Node,
        results: Any | tuple[Any, ...]
    ) -> None:
        available_slots = {
            name: slot for name, slot in node.output_slots.items()
            if not slot.is_disabled()
        }

        if not isinstance(results, tuple):
            results = (results,)

        if len(results) < len(available_slots):
            raise NodeIOError(
                f"Number of non-disabled output slots ({len(available_slots)}) "
                f"is greater than the number of returns ({len(results)}), "
                f"for the node '{repr(node)}'."
            )
        for (name, output_slot), data in zip(available_slots.items(), results):
            if output_slot.is_active():
                databox = supply_ctx.pop(id(output_slot), None)
                if databox is None:
                    raise NodeIOError(
                        f"No databox found for the output slot '{repr(node)}.{name}', "
                        "construct the databox sharing first."
                    )
                if databox.has_data:
                    raise NodeIOError(
                        f"The databox for output '{repr(node)}.{name}' already has data, "
                        "make sure the there are no repeatitive calculations."
                    )
                databox.put(data)


class NahidaRunningContext:
    supply_ctx: dict[int, DataBox]
    demand_ctx: dict[int, list[DataBox]]

    def __init__(self):
        self.supply_ctx = {}
        self.demand_ctx = {}

    def construct_supply_demand(self, nodes: Iterable[Node], group: NodeGroup | None = None) -> None:
        """Construct the data box sharing context for a list of nodes."""
        for node in nodes:
            if not isinstance(node, Node):
                raise TypeError(f"Expected a Node instance, got {type(node)}")

            for output_slot in node.output_slots.values():
                NahidaCtxOperator.create_box(self.supply_ctx, output_slot)

            for input_slot in node.input_slots.values():
                NahidaCtxOperator.share_box(
                    self.supply_ctx, self.demand_ctx,
                    input_slot.source_list, input_slot
                )

        if group:
            for output_slot in group.output_slots.values():
                NahidaCtxOperator.share_box(
                    self.supply_ctx, self.demand_ctx,
                    output_slot.source_list, output_slot
                )

    def receive_data(self, node: Node, positional: list, keyword: dict[str, Any]) -> None:
        """Receive data from the context for a specific node, populating positional and keyword arguments."""
        return NahidaCtxOperator.receive_data(self.demand_ctx, node, positional, keyword)

    def send_data(self, node: Node, results: Any | tuple[Any, ...]) -> None:
        """Send data from a node to the context, storing it in the appropriate data boxes."""
        return NahidaCtxOperator.send_data(self.supply_ctx, node, results)

    def clear(self) -> None:
        """Clear the context, removing all data boxes."""
        self.supply_ctx.clear()
        self.demand_ctx.clear()

    def get(self, group: NodeGroup) -> dict[str, Any]:
        results = {}

        for name, outslot in group.output_slots.items():
            context_key = id(outslot)

            if context_key in self.demand_ctx:
                databox_list = self.demand_ctx.pop(context_key)
                data = tuple(databox.get() for databox in databox_list)
                # NOTE: Can this be variable?
                assert len(data) == 1
                results[name] = data[0]
            elif outslot.has_default:
                results[name] = outslot.default
            else:
                raise NodeIOError

        return results