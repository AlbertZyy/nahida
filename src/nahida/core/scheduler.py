from __future__ import annotations

__all__ = ["Scheduler"]

from typing import Any, Sequence
from dataclasses import dataclass
from collections import deque

from .node import FlowCtrl, Node, TaskItem
from .executor import WorkID, Executor


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
    def __init__(self, *, executor: Executor, max_inflight: int = 1000) -> None:
        """
        Args:
            executor (Executor): the executor.
            max_inflight (int): maximum number of tasks to be executed in parallel.
        """
        self.executor = executor
        self.max_inflight = max_inflight

    def forward(self, context: dict[int, Any], starters: Sequence[Node]) -> dict[int, Any]:
        """Forward computation of a node graph.

        Args:
            context (dict[int, Any]): The context where the nodes read their
                inputs and write their outputs.
            starters (Sequence of Node): The starting nodes.
        """
        # TODO: add checking for circular dependencies!
        scope_manager = ScopeManager(len(starters))
        ready_nodes: deque[tuple[Node, int]] = deque()

        for n in starters:
            ready_nodes.append((n, 0))

        inflight: dict[WorkID, tuple[Node, int, TaskItem]] = {}

        while True:
            while ready_nodes and len(inflight) < self.max_inflight:
                node, scope_id = ready_nodes.popleft()
                task_item = node.submit(context)

                if task_item.target is None:
                    self._recruit_downstreams_and_recall_if_scope_done(
                        ready_nodes, scope_manager, task_item, node, scope_id
                    )
                    continue

                wid = self.executor.submit(task_item.target, *task_item.args, **task_item.kwargs)
                inflight[wid] = (node, scope_id, task_item)

            if len(inflight) == 0:
                if len(ready_nodes) == 0:
                    break
                continue

            event = self.executor.wait()

            if event.work_id is None: # executor-level events
                if event.is_shutdown():
                    break
            else:
                node, scope_id, task_item = inflight.pop(event.work_id)

                if event.is_success():
                    node.write(context, event.value)
                    self._recruit_downstreams_and_recall_if_scope_done(
                        ready_nodes, scope_manager, task_item, node, scope_id
                    )
                elif event.is_failed() or event.is_cancelled():
                    scope_manager.on_node_complete(scope_id)
                    self._recall_if_scope_done(ready_nodes, scope_manager, scope_id)

        return context

    @classmethod
    def _recruit_downstreams_and_recall_if_scope_done(
        cls,
        ready_nodes: deque[tuple[Node, int]],
        scope_manager: ScopeManager,
        task_item: TaskItem,
        node: Node,
        scope_id: int
    ) -> None:
        # If already been cancelled by other nodes, do nothing
        if scope_manager.check_scope_done(scope_id):
            return

        if task_item.control == FlowCtrl.ENTER:
            scope_id = scope_manager.create_scope(scope_id, node)

        elif task_item.control == FlowCtrl.EXIT:
            scope_manager.cancel_scope(scope_id)
            recall_and_back_id = scope_manager.get_recall(scope_id)

            if recall_and_back_id:
                recall, scope_id = recall_and_back_id
                recall.exit()

        elif task_item.control == FlowCtrl.NONE:
            scope_manager.on_node_complete(scope_id)

        else:
            raise ValueError(f"Invalid control flow: {task_item.control!r}")

        if task_item.recruit:
            scope_manager.on_recruit(scope_id, len(task_item.recruit))
            for nxt in task_item.recruit:
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
