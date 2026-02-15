from __future__ import annotations

__all__ = ["NodeChain"]

from .core.node import Node, _Recruiter


class NodeChain:
    """Head and tail of a node path, used for chaining nodes together."""
    def __init__(self, head: _Recruiter, tail: Node, /) -> None:
        self.head = head
        self.tail = tail

    def __rshift__(self, other: NodeChain | Node, /) -> NodeChain:
        if not isinstance(self.tail, _Recruiter):
            raise TypeError("Tail of the self chain must be a Recruiter.")

        if isinstance(other, Node):
            self.tail.link(other)
            return NodeChain(self.head, other)
        elif isinstance(other, NodeChain):
            if not isinstance(other.head, Node):
                raise TypeError("Head of the other chain must be a Node.")
            self.tail.link(other.head)
            return NodeChain(self.head, other.tail)

        return NotImplemented

    def __rrshift__(self, other: NodeChain | _Recruiter, /) -> NodeChain:
        if not isinstance(self.head, Node):
            raise TypeError("Head of the self chain must be a Node.")

        if isinstance(other, _Recruiter):
            other.link(self.head)
            return NodeChain(other, self.tail)
        elif isinstance(other, NodeChain):
            if not isinstance(other.tail, _Recruiter):
                raise TypeError("Tail of the other chain must be a Recruiter.")
            other.tail.link(self.head)
            return NodeChain(other.head, self.tail)

        return NotImplemented
