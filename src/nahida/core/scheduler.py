from __future__ import annotations

__all__ = ["Scheduler"]

from typing import Any, Sequence
from dataclasses import dataclass, replace
from concurrent.futures import (
    Executor,
    Future,
    wait,
    FIRST_COMPLETED
)
from collections import deque

from .node import FlowCtrl, Node, TaskItem


@dataclass(slots=True, frozen=True)
class NodeExecLevel:
    count: int = 1
    exiter: Node | None = None


def _clone_levels(levels: list[NodeExecLevel]) -> list[NodeExecLevel]:
    # levels contains frozen elements, thus a shallow copy is enough
    return list(levels)

def _set_top(levels: list[NodeExecLevel], new_top: NodeExecLevel) -> list[NodeExecLevel]:
    out = list(levels)
    out[-1] = new_top
    return out


class Scheduler:
    def __init__(self, *, executor: Executor, max_inflight: int = 1000) -> None:
        """
        Args:
            executor: executor in `concurrent.futures`, such as
                ThreadPoolExecutor or ProcessPoolExecutor.
            max_inflight (int): maximum number of tasks to be executed in parallel.
        """

        self.executor = executor
        self.max_inflight = max_inflight

    def forward(self, context: dict[int, Any], starters: Sequence[Node]) -> None:
        """Forward computation of a node graph.

        Args:
            context (dict[int, Any]): The context where the nodes read their
                inputs and write their outputs.
            starters (Iterable[Node]): The starting nodes.
        """
        # TODO: add checking for circular dependencies!
        global_level = NodeExecLevel(count=len(starters), exiter=None)
        ready: deque[tuple[Node, list[NodeExecLevel]]] = deque()

        for n in starters:
            ready.append((n, [global_level]))

        inflight: dict[Future, tuple[Node, list[NodeExecLevel], TaskItem]] = {}

        while True:
            while ready and len(inflight) < self.max_inflight:
                node, levels = ready.popleft()
                task_item = node.submit(context)

                if task_item.target is None:
                    Scheduler._schedule(ready, task_item, node, levels)
                    continue

                fut = self.executor.submit(task_item.target, *task_item.args, **task_item.kwargs)
                inflight[fut] = (node, levels, task_item)

            if len(inflight) == 0:
                if len(ready) == 0:
                    break
                continue

            done, _ = wait(inflight.keys(), return_when=FIRST_COMPLETED)

            for fut in done:
                node, levels, task_item = inflight.pop(fut)

                try:
                    results = fut.result()
                except Exception as e:
                    raise RuntimeError(f"Node {node} failed") from e

                node.write(context, results)
                Scheduler._schedule(ready, task_item, node, levels)

    @staticmethod
    def _schedule(
        ready: deque[tuple[Node, list[NodeExecLevel]]],
        task_item: TaskItem,
        node: Node,
        levels: list[NodeExecLevel]
    ):
        if task_item.recruit:
            top = levels[-1]
            levels = _set_top(levels, replace(top, count=top.count + len(task_item.recruit)))
            for nxt in task_item.recruit:
                ready.append((nxt, _clone_levels(levels)))

        if task_item.control == FlowCtrl.REPEAT:
            # start a new level of loop
            levels = _clone_levels(levels)
            levels.append(NodeExecLevel(count=1, exiter=node))

        elif task_item.control == FlowCtrl.BREAK:
            top = levels[-1]
            levels = _set_top(levels, replace(top, count=0))

        elif task_item.control == FlowCtrl.NONE:
            top = levels[-1]
            levels = _set_top(levels, replace(top, count=top.count - 1))

        else:
            raise ValueError(f"Invalid control flow: {task_item.control!r}")

        while levels and levels[-1].count < 1:
            exhausted = levels[-1]
            levels = levels[:-1]
            if exhausted.exiter is not None:
                ready.append((exhausted.exiter, _clone_levels(levels)))
