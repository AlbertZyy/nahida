from typing import Any, Literal
from collections.abc import Callable, Iterable
from collections import deque
import time

from ._types import Node, NodeExceptionData
from .context import NahidaRunningContext

__all__ = ["Graph"]


def collect_relevant_nodes(nodes: Iterable[Node]):
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


def topological_sort(nodes: Iterable[Node]) -> list[Node]:
    in_degree, adj_list = collect_relevant_nodes(nodes)

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


class Graph:
    context: NahidaRunningContext
    requests: Iterable[Node]
    error_listeners: list[Callable[[NodeExceptionData], Any]]

    def __init__(self, requests: Iterable[Node]):
        self.context = NahidaRunningContext()
        self.requests = requests
        self.error_listeners = []

    def _send_exception(self, info: NodeExceptionData) -> None:
        for callback in self.error_listeners:
            callback(info)

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

    def execute(self) -> Literal[0, 1]:
        topological_sorted = topological_sort(self.requests)
        self.context.construct_supply_demand(topological_sorted)

        for node in topological_sorted:
            args, kwargs = [], {}
            try:
                self.context.receive_data(node, args, kwargs)
                results = node.run(*args, **kwargs)
                self.context.send_data(node, results)
            except Exception as error:
                error_info = self._capture_error(node, error, args, kwargs)
                self._send_exception(error_info)
                return 1
        else:
            return 0

    def reset(self) -> None:
        self.context.clear()

    def register_error_hook(self, callback: Callable[[NodeExceptionData], Any]):
        self.error_listeners.append(callback)
