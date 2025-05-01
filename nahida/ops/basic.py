from operator import add, mul

from ..node import Node

__all__ = [
    "AddNode",
    "MulNode"
]


class AddNode(Node):
    def __init__(self, a=0., b=0.):
        super().__init__()
        self.add_input("a", default=a)
        self.add_input("b", default=b)
        self.add_output("out")

    def run(self, a, b):
        return add(a, b)


class MulNode(Node):
    def __init__(self, a=1., b=1.):
        super().__init__()
        self.add_input("a", default=a)
        self.add_input("b", default=b)
        self.add_output("out")

    def run(self, a, b):
        return mul(a, b)