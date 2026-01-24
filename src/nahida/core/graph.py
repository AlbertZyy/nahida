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

from . import _objbase as _ob
from . import expr as _expr
from . import errors as _err
from .context import Context
from .node import Node, FlowCtrl as _FC
from .scheduler import Scheduler

Expr = _expr.Expr

type ForwardFunc = Callable[[Context, Sequence[Node]], Context]


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


class Graph(_ob.NameMixin, _ob.UIDMixin):
    """General computational node graph."""
    def __init__(
        self,
        starters: Sequence[Node],
        exposes: Expr | tuple[Expr, ...] | dict[str, Expr] | None = None,
        *,
        uid: int | None = None
    ):
        """
        Args:
            starters (Iterable of Node): The root nodes for execution.
            exposes (Node, Expr, tuple, dict): Subscribed for outputs.
        """
        _ob.UIDMixin.__init__(self, uid=uid)
        self._starters = starters
        self._expose: Expr | tuple[Expr, ...] | dict[str, Expr] | None = None

        if exposes is None:
            return
        elif isinstance(exposes, Expr):
            self._expose = exposes
        elif isinstance(exposes, tuple):
            self._expose = tuple(map(self._validate_port, exposes))
        elif isinstance(exposes, dict):
            self._expose = {
                key: self._validate_port(value)
                for key, value in exposes.items()
            }
        else:
            raise TypeError(
                "expected expressions, tuple, dict, or None, "
                f"got {type(exposes).__name__!r}."
            )

        self._construct_output = self._build_exposer(exposes)

    def _validate_port(self, port: Expr) -> Expr:
        if _expr.is_expr(port):
            return port
        else:
            raise TypeError(
                f"expected expressions, got {type(port).__name__!r}."
            )

    def _read_context(self, context: Context, expr: Expr, expose_item: Any = None):
        try:
            val = expr.eval(context)
        except Exception as e:
            raise _err.ExposingError(self, expose_item) from e

        return val

    def _build_exposer(
        self,
        exposes: Expr | tuple[Expr, ...] | dict[str, Expr] | None
    ) -> Callable[[Context], Any]:
        if exposes is None:
            def _output_constructor(context): # type: ignore
                return None
            return _output_constructor

        elif isinstance(exposes, Expr):
            exposed_exprs = exposes
            def _output_constructor(context): # type: ignore
                return self._read_context(context, exposes)
            return _output_constructor

        elif isinstance(exposes, tuple):
            exposed_exprs = tuple(map(self._validate_port, exposes))
            def _output_constructor(context): # type: ignore
                return tuple(
                    self._read_context(context, expr, index)
                    for index, expr in enumerate(exposed_exprs)
                )
            return _output_constructor

        elif isinstance(exposes, dict):
            exposed_exprs = {
                key: self._validate_port(value)
                for key, value in exposes.items()
            }
            def _output_constructor(context):
                return {
                    key: self._read_context(context, value, key)
                    for key, value in exposed_exprs.items()
                }
            return _output_constructor

        else:
            raise TypeError(
                "expected expressions, tuple, dict, or None, "
                f"got {type(exposes).__name__!r}."
            )

    def lambdify(self, *, scheduler: Scheduler | None = None):
        """Transform the graph to a lambda function.

        Args:
            scheduler (Scheduler): The forward method of a scheduler should
                receives a context and a sequence of starters, and
                returns the result context (inplace operations allowed).
                Defaults to `None`, using the global default scheduler.

        Returns:
            Callable: The lambda function.
        """
        def runner(*args, **kwargs):
            context = Context()
            if args or kwargs:
                initial: dict[int | str, Any] = {}
                initial.update(enumerate(args))
                initial.update(kwargs)
                context.write(self.uid, initial)

            if scheduler is None:
                raise NotImplementedError # TODO: use the global

            context = scheduler.forward(context, self._starters)
            return self._construct_output(context)

        return runner

    @property
    def input(self):
        return _expr.VariableExpr(self.uid)


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
