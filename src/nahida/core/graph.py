
__all__ = [
    "CircularRecruitmentError",
    "execute",
    "Graph",
    "GraphThread"
]

from threading import Thread
from queue import Queue
from typing import Any
from collections.abc import Iterable, Callable

from .errors import *
from .node import Node, PortId, PortOrNode, FlowCtrl as _FC


def execute(
    context: dict[int, Any], starters: Iterable[Node]
) -> dict[int, Any]:
    """Execute nodes with given inputs.

    Args:
        context (dict[int, Any]): Running context.
        starters (Iterable[Node]): The starting nodes to execute.

    Returns:
        dict: The remaining context, mapping from PortId to their values.
    """
    exec_stack: list[Node] = list(starters)
    loop_stack: list[Node] = []
    trace_stack: list[set[int]] = [{id(node)} for node in exec_stack]
    breaking = False

    while exec_stack:
        node = exec_stack.pop()
        trace = trace_stack.pop()

        if breaking and loop_stack and node is loop_stack[-1]:
            loop_stack.pop()
            breaking = False
            continue

        task = node.submit(context)

        if task.target is not None:
            result = task.target(*task.args, **task.kwargs)
            node.write(context, result)

        if task.control == _FC.REPEAT:
            exec_stack.append(node)
            loop_stack.append(node)
        elif task.control == _FC.BREAK:
            breaking = True

        if task.recruit is None:
            continue

        for next_node in task.recruit:
            next_id = id(next_node)
            if next_id in trace:
                raise CircularRecruitmentError(node, next_node)

            exec_stack.append(next_node)
            trace_stack.append(trace | {next_id})

    return context


class Graph[**P, R]:
    def __init__(
        self,
        starters: Iterable[Node],
        exposes: PortOrNode | tuple[PortOrNode] | dict[str, PortOrNode] | None = None,
        *,
        stub: Callable[P, R] | None = None
    ):
        self._starters = starters
        self._expose: PortId | tuple[PortId, ...] | dict[str, PortId] | None = None
        self._stub = stub

        if exposes is None:
            return
        elif isinstance(exposes, (PortId, Node)):
            self._expose = self._validate_port(exposes)
        elif isinstance(exposes, tuple):
            self._expose = tuple(map(self._validate_port, exposes))
        elif isinstance(exposes, dict):
            self._expose = {
                key: self._validate_port(value)
                for key, value in exposes.items()
            }
        else:
            raise TypeError(
                "Expected PortId, Node, tuple, dict, or None, "
                f"got {type(exposes).__name__!r}."
            )

    def input(self, index_or_key: Any = None, /) -> PortId:
        return PortId(id(self), index_or_key)

    def _validate_port(self, port: PortOrNode) -> PortId:
        if isinstance(port, Node):
            return PortId(id(port), None)
        elif isinstance(port, PortId):
            return port
        else:
            raise TypeError(
                f"Expected PortId or Node, got {type(single).__name__!r}."
            )

    def _read_context(self, context: dict[int, Any], port_id: PortId, name: Any = None):
        node_id, index = port_id
        try:
            val = context[node_id]
        except KeyError:
            raise ExposedNotFoundError(self, name)

        if index is not None:
            val = val[index]

        return val

    def _construct_output(self, context: dict[int, Any]) -> Any:
        # Get value for an input from the context.
        if isinstance(self._expose, PortId):
            return self._read_context(context, self._expose)

        elif isinstance(self._expose, tuple):
            result = []
            for index, port_id in enumerate(self._expose):
                result.append(self._read_context(context, port_id))
            return tuple(result)

        elif isinstance(self._expose, dict):
            result = {}
            for key, port_id in self._expose.items():
                result[key] = self._read_context(context, port_id)
            return result

        return None

    def forward(self, *args: P.args, **kwargs: P.kwargs) -> R:
        if args or kwargs:
            initial: dict[int | str, Any] = {}
            initial.update(enumerate(args))
            initial.update(kwargs)
            context = {id(self): initial}
        else:
            context = {}

        context = execute(context, self._starters)
        return self._construct_output(context)


class GraphThread(Thread):
    def __init__(self, listeners: list[Node], event_queue: Queue):
        super().__init__()
        self.listeners = listeners
        self.event_queue = event_queue

    def run(self) -> None:
        while True:
            event = self.event_queue.get()

            if event is None:
                break

            self._handle_event(event)
            self.event_queue.task_done()

    def _handle_event(self, event: Any):
        """Precess an incoming event."""
        starters = self._get_starters_for_event(event)

        if starters:
            execute({}, starters)

    def _get_starters_for_event(self, event: Any) -> list[Node]:
        """Filter listeners for an event."""
        # TODO: Implement
        return self.listeners

    def stop(self):
        """Stop the thread."""
        self.running = False
        self.event_queue.put(None)
