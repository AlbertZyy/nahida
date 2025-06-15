from typing import Set, Dict, List, Iterable, Mapping, Any, Callable
from collections import deque

from .._types import (
    SlotStatus, DataBox, InputSlot,
    Node, NodeExceptionData, NodeIOError,
    GraphStatus, GraphStatusError
)

__all__ = ["Graph", "WORLD_GRAPH"]


class Graph():
    status : GraphStatus
    context : Dict[Any, DataBox]
    output_nodes : Dict[str, Node]
    error_listeners : List[Callable[[NodeExceptionData], Any]]
    status_listeners : List[Callable[[GraphStatus], Any]]

    def __init__(self):
        self.status = GraphStatus.READY
        self.context = {}
        self.output_nodes = {}
        self.error_listeners = []
        self.status_listeners = []

    @staticmethod
    def _get_input_box(context, slots: Mapping[str, InputSlot]) -> None:
        for name, input_slot in slots.items():
            slot_status = input_slot.status
            if slot_status == SlotStatus.DISABLED:
                continue
            src_node = input_slot.source_node
            src_slot = input_slot.source_slot

            if (src_node is not None) and (slot_status == SlotStatus.ACTIVE):
                context_key = src_node.dump_key(src_slot)

                if context_key in context:
                    input_slot.databox = context[context_key]
                else:
                    raise NodeIOError("Input data box is not found.")

    @staticmethod
    def _put_output_box(context, node: Node) -> None:
        slots = node.output_slots

        for name, output_slot in slots.items():
            if output_slot.status != SlotStatus.DISABLED:
                context_key = node.dump_key(name)
                data_box = DataBox()
                context[context_key] = data_box
                output_slot.databox = data_box

    def _send_exception(self, info: NodeExceptionData) -> None:
        for callback in self.error_listeners:
            callback(info)

    def _set_status(self, status: GraphStatus) -> None:
        if status != self.status:
            self.status = status
            for callback in self.status_listeners:
                callback(self.status)

    def execute(self, retain_data=False) -> None:
        if self.status != GraphStatus.READY:
            raise GraphStatusError("Can only execute when graph is ready.")

        self._set_status(GraphStatus.RUN)
        topological_sorted = self._topological_sort(self.output_nodes.values())

        for node in topological_sorted:
            self._put_output_box(self.context, node)
            self._get_input_box(self.context, node.input_slots)

        if not retain_data:
            self.context.clear()

        for node in topological_sorted:
            error_info = node.execute()
            if error_info is not None:
                self._set_status(GraphStatus.DEBUG)
                self._send_exception(error_info)
                break
        else:
            self._set_status(GraphStatus.STOP)

    def reset(self) -> None:
        self._set_status(GraphStatus.READY)
        self.context.clear()

    @staticmethod
    def _collect_relevant_nodes(nodes: Iterable[Node]):
        """Collect all relevant upstream nodes from output nodes."""
        stack = list(nodes)
        visited   : Set[Node]              = set()
        in_degree : Dict[Node, int]        = {}
        adj_list  : Dict[Node, List[Node]] = {}

        while stack:
            current = stack.pop()

            if current in visited:
                continue

            visited.add(current)
            in_degree[current] = 0

            if current not in adj_list:
                adj_list[current] = []

            for in_slot in current.input_slots.values():
                if in_slot.source_node is not None:
                    source_node = in_slot.source_node
                    stack.append(source_node)
                    in_degree[current] += 1

                    if source_node not in adj_list:
                        adj_list[source_node] = []

                    adj_list[source_node].append(current)

        return in_degree, adj_list

    @staticmethod
    def _topological_sort(nodes: Iterable[Node]) -> List[Node]:
        in_degree, adj_list = Graph._collect_relevant_nodes(nodes)

        queue = deque(node for node, degree in in_degree.items() if degree == 0)
        sorted_nodes = []

        while queue:
            current = queue.popleft()
            sorted_nodes.append(current)
            for neighbor in adj_list[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(sorted_nodes) != len(adj_list):
            raise ValueError("There is a circular dependency in the computation graph.")

        return sorted_nodes

    def OutputPort(self, name: str, /):
        if name in self.output_nodes:
            raise KeyError(f"name `{name}` already exists as an output node.")
        else:
            from .node import OutputNode
            node = OutputNode()
            self.output_nodes[name] = node
            return node.IN

    def Print(self, name: str, /):
        if name in self.output_nodes:
            raise KeyError(f"name `{name}` already exists as an output node.")
        from .node import Node
        node = Node(lambda val: print(val), ("val",), {"val": None})
        self.output_nodes[name] = node
        return node.IN

    def register_error_hook(self, callback: Callable[[NodeExceptionData], Any]):
        self.error_listeners.append(callback)

    def register_status_change_hook(self, callback: Callable[[GraphStatus], Any]):
        self.status_listeners.append(callback)

    __getitem__ = OutputPort


WORLD_GRAPH = Graph()