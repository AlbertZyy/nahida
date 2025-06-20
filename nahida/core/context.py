from typing import Iterable, Tuple, List, Dict, Any
import inspect

from .._types import DataBox, NodeIOError, Node

_PK = inspect._ParameterKind  # type: ignore[reportPrivateUsage]


class NahidaCtxOperator:
    @staticmethod
    def construct_databox_sharing(node: Node, recognition_ctx: Dict, sharing_ctx: Dict) -> None:
        for name, output_slot in node.output_slots.items():
            if output_slot.is_active():
                context_key = node.dump_key(name)
                data_box = DataBox()
                recognition_ctx[context_key] = data_box
                sharing_ctx[node.dump_key(name)] = data_box

        for name, input_slot in node.input_slots.items():
            if input_slot.is_connected():
                context_key = input_slot.source_node.dump_key(input_slot.source_slot)

                if context_key in recognition_ctx:
                    data_box = recognition_ctx[context_key]
                    sharing_ctx[node.dump_key(name)] = data_box
                else:
                    raise NodeIOError("Input data box is not found.")

    @staticmethod
    def receive_data(
        sharing_ctx: Dict[Any, DataBox],
        node: Node,
        positional_only: List,
        keyword: Dict[str, Any]
    ) -> None:
        for name, input_slot in node.input_slots.items():
            if input_slot.is_disabled():
                continue

            if input_slot.is_connected():
                databox = sharing_ctx.pop(node.dump_key(name), None)
                if databox is None:
                    raise NodeIOError(f"No databox found for the input '{repr(node)}.{name}', "
                                      "construct the databox sharing first.")
                if not databox.has_data:
                    raise NodeIOError(f"The databox for input '{repr(node)}.{name}' is empty, "
                                      f"make sure the calculation order is appropriate.")
                data = databox.get()
            elif input_slot.has_default:
                data = input_slot.default
            elif input_slot.param_kind in (_PK.VAR_KEYWORD, _PK.VAR_POSITIONAL):
                continue # variable arguments can be empty
            else:
                raise NodeIOError(f"The input '{repr(node)}.{name}' is "
                                  "not connected and not having a default value.")

            param_kind = input_slot.param_kind
            if param_kind == _PK.POSITIONAL_ONLY:
                positional_only.append(data)
            elif param_kind == _PK.VAR_POSITIONAL:
                assert isinstance(data, tuple)
                positional_only.extend(data)
            elif param_kind in (_PK.POSITIONAL_OR_KEYWORD, _PK.KEYWORD_ONLY):
                keyword[name] = data
            elif param_kind == _PK.VAR_KEYWORD:
                assert isinstance(data, dict)
                keyword.update(data)
            else:
                raise RuntimeError("Inappropriate parameter kind")

    @staticmethod
    def send_data(
        sharing_ctx: Dict[Any, DataBox],
        node: Node,
        results: Any | Tuple[Any, ...]
    ) -> None:
        if not isinstance(results, tuple):
            results = (results,)

        available_slots = {
            name: slot for name, slot in node.output_slots.items()
            if not slot.is_disabled()
        }
        if len(results) != len(available_slots):
            raise NodeIOError(
                f"Number of non-disabled output slots ({len(available_slots)}) "
                f"must match the number of returns ({len(results)}), "
                f"for the node '{repr(node)}'."
            )
        for (name, output_slot), data in zip(available_slots.items(), results):
            if output_slot.is_active():
                databox = sharing_ctx.pop(node.dump_key(name), None)
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


class NahidaRunningContext():
    sharing_ctx: Dict[Any, DataBox]

    def __init__(self):
        self.sharing_ctx = {}

    def construct_databox_sharing(self, nodes: Iterable[Node]) -> None:
        """Construct the data box sharing context for a list of nodes."""
        recognition_ctx: Dict[Any, DataBox] = {}

        for node in nodes:
            if not isinstance(node, Node):
                raise TypeError(f"Expected a Node instance, got {type(node)}")
            NahidaCtxOperator.construct_databox_sharing(node, recognition_ctx, self.sharing_ctx)

    def receive_data(self, node: Node, positional_only: List, keyword: Dict[str, Any]) -> None:
        """Receive data from the context for a specific node, populating positional and keyword arguments."""
        return NahidaCtxOperator.receive_data(self.sharing_ctx, node, positional_only, keyword)

    def send_data(self, node: Node, results: Any | Tuple[Any, ...]) -> None:
        """Send data from a node to the context, storing it in the appropriate data boxes."""
        return NahidaCtxOperator.send_data(self.sharing_ctx, node, results)

    def clear(self) -> None:
        """Clear the context, removing all data boxes."""
        self.sharing_ctx.clear()
