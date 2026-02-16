# nahida/core/graph.py

__all__ = [
    "Graph",
    "GraphThread"
]

from collections.abc import Sequence, Callable
from queue import Queue
from threading import Thread
from typing import Any

from . import _objbase as _ob
from . import errors as _err
from . import expr as _expr
from . import node as _node
from .context import Context
from .executor import Executor
from .scheduler import Scheduler

Expr = _expr.Expr


class Graph(_ob.NameMixin, _ob.UIDMixin):
    """General computational node graph."""
    GRAPH_INPUT_ID = 0
    def __init__(
        self,
        starters: Sequence[_node.Node],
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

        if exposes is not None:
            if isinstance(exposes, Expr):
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

    def lambdify(
        self,
        *,
        scheduler: Scheduler | None = None,
        executor: Executor | None = None
    ):
        """Transform the graph to a lambda function.

        Args:
            scheduler (Scheduler, optional): The scheduler defining the
                execution flow.
                Defaults to `None`, using the global default.
            executor (Executor, optional): The executor running the works
                submitted by the scheduler.
                Defaults to `None`, using the global default.

        Returns:
            Callable: The lambda function.
        """
        if scheduler is None:
            raise NotImplementedError # TODO: use the global
        if executor is None:
            raise NotImplementedError

        def runner(*args, **kwargs):
            context = Context()
            if args or kwargs:
                initial: dict[int | str, Any] = {}
                initial.update(enumerate(args))
                initial.update(kwargs)
                context[self.GRAPH_INPUT_ID] = context.new(initial)

            context = scheduler.forward(
                context,
                [n.activate for n in self._starters],
                executor=executor
            )
            return self._construct_output(context)

        return runner

    def group(self, *, uid: int | None = None) -> _node.Group:
        """Create a new Group node wrapping this graph."""
        return _node.Group(
            guid=self.GRAPH_INPUT_ID,
            entries=set(s.activate for s in self._starters),
            extractor=self._construct_output,
            uid=uid
        )

    @property
    def input(self):
        return _expr.VariableExpr(self.GRAPH_INPUT_ID)


class GraphThread(Thread):
    def __init__(
        self,
        listeners: list[_node.Node],
        event_queue: Queue,
        *,
        scheduler: Scheduler | None = None,
        executor: Executor | None = None
    ):
        super().__init__()
        self.listeners = listeners
        self.event_queue = event_queue

        if scheduler is None:
                raise NotImplementedError # TODO: use the global
        if executor is None:
            raise NotImplementedError

        self._sch = scheduler
        self._exe = executor

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
            context = self._sch.forward(Context(), starters, executor=self._exe)

    def _get_starters_for_event(self, event: Any) -> list[_node.Node]:
        """Filter listeners for an event."""
        # TODO: Implement
        return self.listeners

    def stop(self):
        """Stop the thread."""
        self.running = False
        self.event_queue.put(None)
