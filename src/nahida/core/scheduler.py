from __future__ import annotations

__all__ = [
    "FlowControl",
    "OrderItem",
    "Scheduler",
    "ConcurrentScheduler"
]

from typing import Sequence, Protocol, Any
from dataclasses import dataclass, field
from collections import deque
from collections.abc import Collection, Callable, Generator
from enum import StrEnum

from .context import Context, SimpleDataRef, DataRefFactory
from .expr import Expr
from .executor import TaskID, Executor


class FlowControl(StrEnum):
    """Workflow control instruction after execution."""

    NONE = "none"
    """Do nothing and be removed."""

    ENTER = "enter"
    """**Enter a new scope**

    Require a new scope for downstreams. Recall (recruit again) this node
    when the scope exhausts naturely."""

    EXIT = "exit"
    """**Exit current scope**

    Require cancel the current scope. Call `exit` method of the scope's creator node."""


empty = object()
type Coroutine = Generator[OrderItem, Any, Any]
type CoroutineFunc = Callable[[Context], Coroutine]

@dataclass(slots=True, frozen=True)
class OrderItem:
    """The Order Item consumed by schedulers.

    Args:
        uid (int): A unique ID for saving data in the context.
        release (Any, optional): Data released to the context before execution.
            Note that this value will be covered by execution results, if
            `source` is given.
        source (int | str | None, optional): The function ID or source code to
            submit to an executor.
        args (tuple of Expr, optional): positional arguments for execution.
        kwargs (dict[str, Expr], optional): keyword arguments for execution.
        control (FlowControl, optional): Workflow control instruction **after**
            execution. Defaults to `none`.
        recall (Context -> OrderItem, optional): Make an new OrderItem when the
            scope created exhausts naturely.
            Have effect only for `FlowControl.ENTER`.
        exit (Context -> OrderItem, optional): Make an new OrderItem when the
            scope created was breaked by other nodes.
            Have effect only for `FlowControl.ENTER`.
    """
    uid: int
    context: Context = field(default_factory=Context)
    source: int | str | None = None
    args: tuple[Expr, ...] = field(default_factory=tuple)
    kwargs: dict[str, Expr] = field(default_factory=dict)
    recruit: Collection[Node] | None = None
    control: FlowControl = FlowControl.NONE


class Node(Protocol):
    @property
    def uid(self) -> int: ...
    def activate(self, context: Context, /) -> Coroutine: ...
    def exit(self) -> None: ...


class ScopeState:
    _stack: list[tuple[Coroutine, Coroutine]]
    def push(
        self,
        recall: Coroutine | None = None,
    ): ...
    def pop(self) -> tuple[Coroutine, Coroutine]: ...


@dataclass(slots=True)
class NodeScope:
    count: int = 1
    recall: Coroutine | None = None
    back_id: int | None = None
    cancelled: bool = False


class ScopeManager:
    _scope_count: int
    scope_table: dict[int, NodeScope]

    def __init__(self, num_starters: int) -> None:
        self._scope_count = 1
        self.scope_table: dict[int, NodeScope] = {0: NodeScope(num_starters)}

    def __contains__(self, item: int) -> bool:
        return item in self.scope_table

    def __getitem__(self, item: int) -> NodeScope:
        try:
            return self.scope_table[item]
        except KeyError:
            raise KeyError(f"invalid scope id {item}")

    def create_scope(self, back_id: int, recall: Coroutine) -> int:
        new_id = self._scope_count
        new_scope = NodeScope(0, recall, back_id)
        self.scope_table[new_id] = new_scope
        self._scope_count += 1
        return new_id

    def on_node_complete(self, scope_id: int) -> None:
        self[scope_id].count -= 1

    def on_recruit(self, scope_id: int, n: int) -> None:
        self[scope_id].count += n

    def cancel_scope(self, scope_id: int) -> int:
        self[scope_id].cancelled = True
        back_id = self[scope_id].back_id
        if back_id is None:
            raise RuntimeError("cannot cancel the root scope as "
                               "no back scope found")
        return back_id

    def check_scope_done(self, scope_id: int) -> bool:
        if scope_id not in self.scope_table:
            return False

        scope = self.scope_table[scope_id]
        return scope.count < 1 or scope.cancelled == True

    def get_recall(self, scope_id: int) -> tuple[Coroutine, int] | None:
        scope = self[scope_id]

        if scope.recall is not None:
            assert scope.back_id is not None, "back_id cannot be None if recall was specified"
            return scope.recall, scope.back_id


class Scheduler:
    def forward(self, context: Context, starters: Sequence[Node], *, executor: Executor) -> Context:
        """Forward computation of a node graph.

        Args:
            context (Context): The context where the nodes read their
                inputs and write their outputs.
            starters (Sequence of Node): The starting nodes.
            executor (Executor): An executor that supports submit and wait.
        """
        raise NotImplementedError()


class ConcurrentScheduler(Scheduler):
    def __init__(self, max_inflight: int = 1000, data_ref: DataRefFactory = SimpleDataRef) -> None:
        """A concurrent scheduler.

        Args:
            max_inflight (int): maximum number of tasks to be executed in parallel.
            data_ref (type): A callable returning a DataRef instance with no
                parameters. DataRef refers to instances that have `get` and
                `set` methods to load and save data.
                Defaults to SimpleDataRef that stores values in memory directly.
        """
        self._max_inflight = max_inflight
        self._data_ref_factory = data_ref

    def forward(self, context: Context, starters: Sequence[Node], *, executor: Executor) -> Context:
        from itertools import chain
        # TODO: add checking for circular dependencies!
        scope_manager = ScopeManager(len(starters))
        ready_nodes: deque[tuple[Coroutine, int]] = deque()

        for n in starters:
            ready_nodes.append((n.activate(context), 0))

        inflight: dict[TaskID, tuple[Coroutine, int, OrderItem]] = {}

        while True:
            while ready_nodes and len(inflight) < self._max_inflight:
                coro, scope_id = ready_nodes.popleft()
                try:
                    order_item = next(coro)
                except StopIteration:
                    continue

                if order_item.source is None:
                    self._recruit_downstreams_and_recall_if_scope_done(
                        coro, ready_nodes, scope_manager, order_item, scope_id
                    )
                    continue

                uid_set: set[int] = set()
                for expr in chain(order_item.args, order_item.kwargs.values()):
                    uid_set |= expr.refs()

                wid = executor.submit(
                    order_item.source, order_item.context.view(uid_set),
                    order_item.args, order_item.kwargs
                )
                inflight[wid] = (coro, scope_id, order_item)

            if len(inflight) == 0:
                break

            event = executor.wait()

            if event.task_id is None: # executor-level events
                if event.is_shutdown():
                    break
            else:
                coro, scope_id, order_item = inflight.pop(event.task_id)
                if event.is_success():
                    if event.value is not None:
                        order_item.context[order_item.uid] = event.value
                    self._recruit_downstreams_and_recall_if_scope_done(
                        coro, ready_nodes, scope_manager, order_item, scope_id
                    )
                else:
                    scope_manager.on_node_complete(scope_id)
                    self._recall_if_scope_done(ready_nodes, scope_manager, scope_id)

        return context

    @classmethod
    def _recruit_downstreams_and_recall_if_scope_done(
        cls,
        coro: Coroutine,
        ready_nodes: deque[tuple[Coroutine, int]],
        scope_manager: ScopeManager,
        order_item: OrderItem,
        scope_id: int
    ) -> None:
        # If already been cancelled by other nodes, do nothing
        if scope_manager.check_scope_done(scope_id):
            return

        if order_item.control == FlowControl.ENTER:
            scope_id = scope_manager.create_scope(scope_id, coro)

        elif order_item.control == FlowControl.EXIT:
            scope_id = scope_manager.cancel_scope(scope_id)

        elif order_item.control == FlowControl.NONE:
            scope_manager.on_node_complete(scope_id)

        else:
            raise ValueError(f"Invalid control flow: {order_item.control!r}")

        if order_item.recruit:
            scope_manager.on_recruit(scope_id, len(order_item.recruit))
            for nxt in order_item.recruit:
                ready_nodes.append((nxt.activate(order_item.context), scope_id))

        if order_item != FlowControl.EXIT:
            cls._recall_if_scope_done(ready_nodes, scope_manager, scope_id)

    @classmethod
    def _recall_if_scope_done(
        cls,
        ready_nodes: deque[tuple[Coroutine, int]],
        scope_manager: ScopeManager,
        scope_id: int
    ):
        if scope_manager.check_scope_done(scope_id):
            recall_and_back_id = scope_manager.get_recall(scope_id)
            if recall_and_back_id:
                ready_nodes.append(recall_and_back_id)
