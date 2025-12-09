from typing import Any, Literal
from collections.abc import Callable, Iterable
from collections import deque, OrderedDict
import time

from . import edge as _E
from ._types import Node, NodeExceptionData, InputSlot, TransSlot
from ._types import ParamPassingKind as PPK
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
    _input_slots : OrderedDict[str, InputSlot]
    _output_slots : OrderedDict[str, TransSlot]
    _interior: bool
    context: NahidaRunningContext
    error_listeners: list[Callable[[NodeExceptionData], Any]]

    def __init__(self):
        self._input_slots = OrderedDict()
        self._output_slots = OrderedDict()
        self._interior = False
        self.context = NahidaRunningContext()
        self.error_listeners = []
        # A fake node returning the input data of the graph,
        # because `topological_sort` needs nodes with 0 in-degree as start points.
        self._input_node = GraphInputNode(self, OrderedDict())

    def __bool__(self) -> bool:
        return True

    def register_input(
            self,
            name: str,
            variable: bool = False,
            parameter: str | None = None,
            **kwargs
    ):
        """Add an input slot to the node."""
        if "_input_slots" not in self.__dict__:
            raise AttributeError(
                "cannot assign inputs before Node.__init__() call"
            )
        elif not isinstance(name, str):
            raise TypeError(
                f"input name should be a string, but got {name.__class__.__name__}"
            )
        elif "." in name:
            raise KeyError('input name can\'t contain "."')
        elif name == "":
            raise KeyError('input name can\'t be empty string ""')
        elif hasattr(self, name) and name not in self._input_slots:
            raise KeyError(f"attribute '{name}' already exists")
        else:
            if parameter is None:
                parameter = name
            param_name, param_kind = parameter, PPK.KEYWORD,

            self._input_slots[name] = InputSlot(
                has_default="default" in kwargs,
                default=kwargs.get("default", None),
                param_name=param_name,
                param_kind=param_kind,
                variable=variable
            )

    def register_output(self, name: str, **kwargs):
        """Add an output slot to the node."""
        if "_output_slots" not in self.__dict__:
            raise AttributeError(
                "cannot assign outputs before Node.__init__() call"
            )
        elif not isinstance(name, str):
            raise TypeError(
                f"output name should be a string, but got {name.__class__.__name__}"
            )
        elif "." in name:
            raise KeyError('output name can\'t contain "."')
        elif name == "":
            raise KeyError('output name can\'t be empty string ""')
        elif hasattr(self, name) and name not in self._output_slots:
            raise KeyError(f"attribute '{name}' already exists")
        else:
            self._output_slots[name] = TransSlot(
                has_default="default" in kwargs,
                default=kwargs.get("default", None)
            )

    @property
    def input_slots(self):
        return self._input_slots

    @property
    def output_slots(self):
        return self._output_slots

    def run(self, **kwargs):
        self.execute(kwargs)
        return tuple(self.get().values())

    def __call__(self, **kwargs: _E.AddrHandler):
        if self._interior:
            for name in kwargs.keys():
                if name not in self.output_slots:
                    self.register_output(name)
            _E.connect_from_address(self.output_slots, kwargs)
        else:
            _E.connect_from_address(self.input_slots, kwargs)
        return _E.AddrHandler(self, None, not self._interior)

    def __getitem__(self, slot: str):
        if slot not in self.input_slots:
            self.register_input(slot)
        return _E.AddrHandler(self._input_node, slot)

    def __enter__(self):
        self._interior = True

    def __exit__(self, type, value, trace):
        self._interior = False

    def requests(self):
        request_set: set[Node] = set()

        for outslot in self.output_slots.values():
            request_set = request_set.union(
                set(addr.node for addr in outslot.source_list)
            )

        return request_set

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

    def get(self):
        return self.context.get(self)

    def execute(self, data: dict[str, Any]) -> Literal[0, 1]:
        topological_sorted = topological_sort(self.requests())
        self.context.construct_supply_demand(topological_sorted, self)
        self._input_node.data.clear()
        self._input_node.data.update(data)

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


class GraphInputNode:
    def __init__(self, graph: Graph, data: OrderedDict[str, Any]):
        self.graph = graph
        self.data = data

    @property
    def input_slots(self):
        return {}

    @property
    def output_slots(self):
        return self.graph.input_slots

    def run(self):
        return tuple(self.data.values())
