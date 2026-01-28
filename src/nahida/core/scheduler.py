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
from collections.abc import Collection
from enum import StrEnum

from .context import Context, SimpleDataRef, DataRefFactory
from .expr import Expr
from .executor import TaskID, Executor


class FlowControl(StrEnum):
    """Workflow control instruction after execution."""

    NONE = "none"
    """Do nothing and be removed from the tasks."""

    ENTER = "enter"
    """Require a new scope for downstreams."""

    EXIT = "exit"
    """Require cancel the current scope."""


empty = object()

@dataclass(slots=True, frozen=True)
class OrderItem:
    source: int | str | None = None
    args: tuple[Expr, ...] = field(default_factory=tuple)
    kwargs: dict[str, Expr] = field(default_factory=dict)
    release: Any = empty
    recruit: Collection[Node] | None = None
    control: FlowControl = FlowControl.NONE


class Node(Protocol):
    @property
    def uid(self) -> int: ...
    def order(self, context: Context, /) -> OrderItem: ...
    def exit(self) -> None: ...


@dataclass(slots=True)
class NodeScope:
    count: int = 1
    recall: Node | None = None
    back_id: int | None = None
    cancelled: bool = False


class ScopeManager:
    _scope_count: int
    scope_table: dict[int, NodeScope]

    def __init__(self, num_starters: int) -> None:
        self._scope_count = 1
        self.scope_table: dict[int, NodeScope] = {0: NodeScope(num_starters)}

    def __getitem__(self, item: int):
        try:
            return self.scope_table[item]
        except KeyError:
            raise ValueError(f"invalid scope id {item}")

    def create_scope(self, back_id: int, recall: Node) -> int:
        new_id = self._scope_count
        new_scope = NodeScope(0, recall, back_id)
        self.scope_table[new_id] = new_scope
        self._scope_count += 1
        return new_id

    def on_node_complete(self, scope_id: int) -> None:
        self[scope_id].count -= 1

    def on_recruit(self, scope_id: int, n: int) -> None:
        self[scope_id].count += n

    def cancel_scope(self, scope_id: int) -> None:
        self[scope_id].cancelled = True

    def check_scope_done(self, scope_id: int) -> bool:
        if scope_id not in self.scope_table:
            return False

        scope = self.scope_table[scope_id]
        return scope.count < 1 or scope.cancelled == True

    def get_recall(self, scope_id: int) -> tuple[Node, int] | None:
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
        ready_nodes: deque[tuple[Node, int]] = deque()

        for n in starters:
            ready_nodes.append((n, 0))

        inflight: dict[TaskID, tuple[Node, int, OrderItem]] = {}

        while True:
            while ready_nodes and len(inflight) < self._max_inflight:
                node, scope_id = ready_nodes.popleft()
                order_item = node.order(context)

                if order_item.release is not empty:
                    context[node.uid] = self._data_ref_factory(order_item.release)

                if order_item.source is None:
                    self._recruit_downstreams_and_recall_if_scope_done(
                        ready_nodes, scope_manager, order_item, node, scope_id
                    )
                    continue

                uid_set: set[int] = set()
                for expr in chain(order_item.args, order_item.kwargs.values()):
                    uid_set |= expr.refs()

                wid = executor.submit(
                    order_item.source, context.view(uid_set),
                    order_item.args, order_item.kwargs
                )
                inflight[wid] = (node, scope_id, order_item)

            if len(inflight) == 0:
                if len(ready_nodes) == 0:
                    break
                continue

            event = executor.wait()

            if event.task_id is None: # executor-level events
                if event.is_shutdown():
                    break
            else:
                node, scope_id, order_item = inflight.pop(event.task_id)
                if event.is_success():
                    if event.value is not None:
                        context[node.uid] = event.value
                    self._recruit_downstreams_and_recall_if_scope_done(
                        ready_nodes, scope_manager, order_item, node, scope_id
                    )
                else:
                    scope_manager.on_node_complete(scope_id)
                    self._recall_if_scope_done(ready_nodes, scope_manager, scope_id)

        return context

    @classmethod
    def _recruit_downstreams_and_recall_if_scope_done(
        cls,
        ready_nodes: deque[tuple[Node, int]],
        scope_manager: ScopeManager,
        order_item: OrderItem,
        node: Node,
        scope_id: int
    ) -> None:
        # If already been cancelled by other nodes, do nothing
        if scope_manager.check_scope_done(scope_id):
            return

        if order_item.control == FlowControl.ENTER:
            scope_id = scope_manager.create_scope(scope_id, node)

        elif order_item.control == FlowControl.EXIT:
            scope_manager.cancel_scope(scope_id)
            recall_and_back_id = scope_manager.get_recall(scope_id)

            if recall_and_back_id:
                recall, scope_id = recall_and_back_id
                recall.exit()

        elif order_item.control == FlowControl.NONE:
            scope_manager.on_node_complete(scope_id)

        else:
            raise ValueError(f"Invalid control flow: {order_item.control!r}")

        if order_item.recruit:
            scope_manager.on_recruit(scope_id, len(order_item.recruit))
            for nxt in order_item.recruit:
                ready_nodes.append((nxt, scope_id))

        cls._recall_if_scope_done(ready_nodes, scope_manager, scope_id)

    @classmethod
    def _recall_if_scope_done(
        cls,
        ready_nodes: deque[tuple[Node, int]],
        scope_manager: ScopeManager,
        scope_id: int
    ):
        if scope_manager.check_scope_done(scope_id):
            recall_and_back_id = scope_manager.get_recall(scope_id)

            if recall_and_back_id:
                ready_nodes.append(recall_and_back_id)
