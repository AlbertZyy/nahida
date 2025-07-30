from typing import Any
from collections.abc import Callable, Iterable
import time
from collections import deque
from enum import IntEnum

from ._types import (
    Node, NodeExceptionData,
    GraphStatusError
)
from .node import inspected
from .context import NahidaRunningContext

__all__ = ["Graph", "WORLD_GRAPH"]


class GraphStatus(IntEnum):
    READY = 0
    RUN   = 1
    DEBUG = 2
    STOP  = 3


class Graph():
    status : GraphStatus
    context : NahidaRunningContext
    output_nodes : list[Node]
    error_listeners : list[Callable[[NodeExceptionData], Any]]
    status_listeners : list[Callable[[GraphStatus], Any]]

    def __init__(self):
        self.status = GraphStatus.READY
        self.context = NahidaRunningContext()
        self.output_nodes = []
        self.error_listeners = []
        self.status_listeners = []

    def _send_exception(self, info: NodeExceptionData) -> None:
        for callback in self.error_listeners:
            callback(info)

    def _set_status(self, status: GraphStatus) -> None:
        if status != self.status:
            self.status = status
            for callback in self.status_listeners:
                callback(self.status)

    @staticmethod
    def _capture_error(node: Node, error: Exception, args, kwargs) -> NodeExceptionData:
        return NodeExceptionData(
                node=node,
                timestamp=time.time(),
                errtype=type(error),
                message=str(error),
                positional_inputs=args,
                keyword_inputs=kwargs
            )

    def execute(self) -> None:
        if self.status != GraphStatus.READY:
            raise GraphStatusError("Can only execute when graph is ready.")

        self._set_status(GraphStatus.RUN)
        topological_sorted = self._topological_sort(self.output_nodes)
        self.context.construct_supply_demand(topological_sorted)

        for node in topological_sorted:
            args, kwargs = [], {}
            try:
                self.context.receive_data(node, args, kwargs)
                results = node.run(*args, **kwargs)
                self.context.send_data(node, results)
            except Exception as error:
                error_info = self._capture_error(node, error, args, kwargs)
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
        visited   : set[Node]              = set()
        in_degree : dict[Node, int]        = {}
        adj_list  : dict[Node, list[Node]] = {}

        while stack:
            current = stack.pop()

            if current in visited:
                continue

            visited.add(current)
            in_degree[current] = 0

            if current not in adj_list:
                adj_list[current] = []

            for in_slot in current.input_slots.values():
                for source in in_slot.source_list:
                    stack.append(source.node)
                    in_degree[current] += 1

                    if source.node not in adj_list:
                        adj_list[source.node] = []

                    adj_list[source.node].append(current)

        return in_degree, adj_list

    @staticmethod
    def _topological_sort(nodes: Iterable[Node]) -> list[Node]:
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

    def register_error_hook(self, callback: Callable[[NodeExceptionData], Any]):
        self.error_listeners.append(callback)

    def register_status_change_hook(self, callback: Callable[[GraphStatus], Any]):
        self.status_listeners.append(callback)

    def register_output_node(self, handler: Callable[..., None]):
        node = inspected(handler, outputs=())
        self.output_nodes.append(node)
        return node


WORLD_GRAPH = Graph()