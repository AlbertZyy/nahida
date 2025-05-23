from typing import Set, Dict, List, Iterable, Any, Callable
from collections import deque
import time

from ._types import (
    Node, InputSlot, OutputSlot,
    NodeExceptionData, NodeIOError,
    GraphStatus, GraphStatusError
)

__all__ = ["Graph", "WORLD_GRAPH"]


class Graph():
    status : GraphStatus
    context : Dict[OutputSlot, Any]
    output_nodes : Dict[str, Node]
    error_listeners : List[Callable[[NodeExceptionData], Any]]
    status_listeners : List[Callable[[GraphStatus], Any]]

    def __init__(self):
        self.status = GraphStatus.READY
        self.context = {}
        self.output_nodes = {}
        self.error_listeners = []
        self.status_listeners = []

    def _get_inputs(self, slots: Iterable[InputSlot]) -> Dict[str, Any]:
        input_data = {}

        for input_slot in slots:
            name = input_slot.name
            src_slot = input_slot.source

            if src_slot is None:
                if input_slot.has_default():
                    input_data[name] = input_slot.default
                else:
                    raise NodeIOError(f"The '{name}' input of node "
                                      f"'{self}' is not connected "
                                       "and not having a default value.")
            elif src_slot in self.context:
                input_data[name] = self.context[src_slot]
            else:
                raise NodeIOError("Input data not found.")

        return input_data

    def _put_outputs(self, slots: Iterable[OutputSlot], results: Any) -> None:
        if not isinstance(results, tuple):
            results = (results,)

        try:
            for output_slot, result in zip(slots, results, strict=True):
                self.context[output_slot] = result
        except ValueError:
            raise NodeIOError(
                f"Number of returns ({len(results)}) mismatch "
                f"the number of output slots."
            )

    @staticmethod
    def _capture_exception(node: Node, exception: Exception, inputs: Dict[str, Any]):
        return NodeExceptionData(
            node=node,
            timestamp=time.time(),
            type=type(exception),
            message=str(exception),
            inputs=inputs
        )

    def _send_exception(self, info: NodeExceptionData) -> None:
        for callback in self.error_listeners:
            callback(info)

    def _set_status(self, status: GraphStatus) -> None:
        if status != self.status:
            self.status = status
            for callback in self.status_listeners:
                callback(self.status)

    def execute(self) -> None:
        if self.status != GraphStatus.READY:
            raise GraphStatusError("Can only execute when graph is ready.")

        self._set_status(GraphStatus.RUN)

        for node in self._topological_sort(self.output_nodes.values()):
            input_data = self._get_inputs(node.input_slots.values())
            try:
                results = node.run(**input_data)
            except Exception as exception:
                self._set_status(GraphStatus.DEBUG)
                error_info = self._capture_exception(node, exception, input_data)
                self._send_exception(error_info)
                break

            self._put_outputs(node.output_slots.values(), results)
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

            for in_slot in current._input_slots.values():
                if in_slot.source is not None:
                    source_node = in_slot.source.node
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
            return self.output_nodes[name].IN
        else:
            from .node import OutputNode
            node = OutputNode()
            self.output_nodes[name] = node
            return node.IN

    def addErrorListener(self, callback: Callable[[NodeExceptionData], Any]):
        self.error_listeners.append(callback)

    def addStatusListener(self, callback: Callable[[GraphStatus], Any]):
        self.status_listeners.append(callback)

    __getitem__ = OutputPort


WORLD_GRAPH = Graph()