from operator import add, mul

from ..core.node import Node

__all__ = [
    "AddNode",
    "MulNode"
]


class AddNode(Node):
    def __init__(self, a=0., b=0.):
        super().__init__()
        self.register_input("a", default=a)
        self.register_input("b", default=b)
        self.register_output("out")

    def run(self, a, b):
        return add(a, b)


class MulNode(Node):
    def __init__(self, a=1., b=1.):
        super().__init__()
        self.register_input("a", default=a)
        self.register_input("b", default=b)
        self.register_output("out")

    def run(self, a, b):
        return mul(a, b)