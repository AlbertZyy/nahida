
__all__ = ["Graph"]

from typing import Any
from .node import Node, PortId, FlowCtrl as _FC


class Graph:
    def __init__(self, starters: list[Node]):
        self.connects: dict[str, PortId] = {}
        self.exec_stack: list[Node] = starters.copy()
        self.loop_stack: list[Node] = []

    def __getitem__(self, item: str):
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

    def read_value(self, context: dict[PortId, Any], name: str) -> tuple[Any, bool]:
        """Get value for an input from the context."""
        if name in self.connects:
            pack_id = self.connects[name]
            if pack_id in context:
                return context[pack_id], True

        return None, False

    def execute(self, **kwargs: Any) -> tuple[Any]:
        context = {PortId(self, name): value for name, value in kwargs.items()}
        breaking = False

        while self.exec_stack:
            node = self.exec_stack.pop()

            if breaking and self.loop_stack and node is self.loop_stack[-1]:
                self.loop_stack.pop()
                breaking = False
                continue

            feedback = node.run(context)

            if feedback.control == _FC.REPEAT:
                self.exec_stack.append(node)
                self.loop_stack.append(node)
            elif feedback.control == _FC.BREAK:
                breaking = True

            if feedback.recruit is None:
                continue

            for next_node in feedback.recruit:
                self.exec_stack.append(next_node)

        results = []

        for key in self.connects:
            val, status = self.read_value(context, key)

            if not status:
                raise RuntimeError(
                    f"Output '{key}' not found in graph execution context."
                )

            results.append(val)

        return tuple(results)
