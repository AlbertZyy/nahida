# nahida/core/graph.py

__all__ = [
    "execute",
    "Graph",
    "GraphThread"
]

from threading import Thread
from queue import Queue
from typing import Any
from collections.abc import Sequence, Callable

from .import expr as _expr
from .import errors as _err
from .node import Node, FlowCtrl as _FC, ExprOrNode

Expr = _expr.Expr

type ForwardFunc = Callable[[dict[int, Any], Sequence[Node]], dict[int, Any]]


def execute(
    context: dict[int, Any], starters: Sequence[Node]
) -> dict[int, Any]:
    """Execute nodes with given inputs.

    Args:
        context (dict[int, Any]): Running context.
        starters (Sequence of Node): The starting nodes to execute.

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

        if task.control == _FC.ENTER:
            exec_stack.append(node)
            trace_stack.append(trace)
            loop_stack.append(node)
        elif task.control == _FC.EXIT:
            breaking = True

        if task.recruit is None:
            continue

        for next_node in task.recruit:
            next_id = id(next_node)
            if next_id in trace:
                raise _err.CircularRecruitmentError(node, next_node)

            exec_stack.append(next_node)
            trace_stack.append(trace | {next_id})

    return context


class Graph:
    """General computational node graph."""
    def __init__(
        self,
        starters: Sequence[Node],
        exposes: ExprOrNode | tuple[ExprOrNode, ...] | dict[str, ExprOrNode] | None = None,
        *,
        gname: str | None = None
    ):
        """
        Args:
            starters (Iterable of Node): The root nodes for execution.
            exposes (Node, Expr, tuple, dict): Subscribed for outputs.
            gname (str | None): Name for the graph.
        """
        self._starters = starters
        self._expose: Expr | tuple[Expr, ...] | dict[str, Expr] | None = None
        self._gname = gname

        if exposes is None:
            return
        elif isinstance(exposes, Node) or _expr.is_expr(exposes):
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

    def __repr__(self) -> str:
        type_name = self.__class__.__name__

        if self._gname:
            return "{}({})".format(type_name, self._gname)

        return "<graph {} at {}>".format(type_name, hex(id(self)))

    @property
    def __name__(self) -> str:
        if self._gname:
            return self._gname
        return repr(self)

    def _validate_port(self, port: ExprOrNode) -> Expr:
        if _expr.is_expr(port):
            return port
        else:
            raise TypeError(
                f"Expected PortId or Node, got {type(port).__name__!r}."
            )

    def _read_context(self, context: dict[int, Any], expr: Expr, expose_item: Any = None):
        try:
            val = expr.eval(context)
        except Exception as e:
            raise _err.ExposingError(self, expose_item) from e

        return val

    def _construct_output(self, context: dict[int, Any]) -> Any:
        # Get value for an input from the context.
        if _expr.is_expr(self._expose):
            return self._read_context(context, self._expose)

        elif isinstance(self._expose, tuple):
            result = []
            for index, expr in enumerate(self._expose):
                result.append(self._read_context(context, expr, index))
            return tuple(result)

        elif isinstance(self._expose, dict):
            result = {}
            for key, expr in self._expose.items():
                result[key] = self._read_context(context, expr, key)
            return result

        return None

    def input(self, name: str | int | None = None) -> Expr:
        expr = _expr.simple_fetcher(id(self))

        if name is not None:
            expr = expr[name]

        return expr

    def run(
        self,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] = {},
        *,
        forward: ForwardFunc | None = None
    ):
        if args or kwargs:
            initial: dict[int | str, Any] = {}
            initial.update(enumerate(args))
            initial.update(kwargs)
            context = {id(self): initial}
        else:
            context = {}

        _forward = forward or execute
        context = _forward(context, self._starters)
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
