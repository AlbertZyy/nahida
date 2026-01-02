
__all__ = [
    "CircularRecruitmentError",
    "execute",
    "Graph",
    "GraphThread"
]

from threading import Thread
from queue import Queue
from typing import Any
from collections.abc import Iterable

from .node import Node, PortId, FlowCtrl as _FC


class CircularRecruitmentError(Exception):
    """Raised when circular recruitment occurs."""


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
                raise CircularRecruitmentError(
                    f"{node!r} wants to recruit {next_node!r} that exists "
                    "in the execution path."
                )

            exec_stack.append(next_node)
            trace_stack.append(trace | {next_id})

    return context


class Graph:
    def __init__(self, starters: Iterable[Node]):
        self.starters = starters
        self.connects: dict[str, PortId] = {}

    def __getitem__(self, item: Any):
        return PortId(self, item)

    def __call__(self, **kwargs: PortId):
        for name, value in kwargs.items():
            if isinstance(value, PortId):
                self.connects[name] = value
            else:
                raise TypeError(
                    f"Graph output {name!r} must connect to a PortId, "
                    f"got {type(value).__name__!r}."
                )

    def read_value(self, context: dict[int, Any], name: str) -> tuple[Any, bool]:
        """Get value for an input from the context."""
        if name in self.connects:
            node_id, index = self.connects[name]
            if node_id in context:
                val = context[node_id]
                if index is not None:
                    val = val[index]
                return val, True

        return None, False

    def forward(self, **kwargs: Any) -> dict[str, Any]:
        context = {id(self): kwargs}
        context = execute(context, self.starters)
        results: dict[str, Any] = {}

        for key in self.connects:
            val, status = self.read_value(context, key)

            if not status:
                raise RuntimeError(
                    f"Output '{key}' not found in graph execution context."
                )

            results[key] = val

        return results


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
