from typing import (
    Tuple, Dict,
    Mapping, MutableMapping,
    Any,
    TypeVar, overload
)
from .connection import InputSlot, OutputSlot

__all__ = [
    "Node",
    "Const",
    "Sequential"
]

_Self = TypeVar("_Self")


class NodeBase(object):
    @property
    def input_slots(self) -> Mapping[str, InputSlot]:
        raise NotImplementedError

    @property
    def output_slots(self) -> Mapping[str, OutputSlot]:
        raise NotImplementedError

    @property
    def IN(self):
        from .utils import _ConnPortIn
        return _ConnPortIn(self.input_slots)

    @property
    def OUT(self):
        from .utils import _ConnPortOut
        return _ConnPortOut(self.output_slots)


class Node(NodeBase):
    _input_slots : Dict[str, InputSlot]
    _output_slots : Dict[str, OutputSlot]

    def __init__(
        self,
        target=None,
        inputs: Tuple[str, ...] = (),
        defaults: Dict[str, Any] = {},
        outputs: Tuple[str, ...] = ()
    ):
        r"""Initialize a compute node."""
        super().__init__()
        self._input_slots = {}
        self._output_slots = {}
        self._target = target

        if target:
            for in_arg in inputs:
                if in_arg in defaults:
                    self.add_input(in_arg, default=defaults[in_arg])
                else:
                    self.add_input(in_arg)

            for out_arg in outputs:
                self.add_output(out_arg)

    def __hash__(self):
        return id(self)

    @overload
    def add_input(self: _Self, name: str) -> _Self: ...
    @overload
    def add_input(self: _Self, name: str, *, default: Any) -> _Self: ...
    def add_input(self, name: str, **kwargs):
        self._input_slots[name] = InputSlot(name, **kwargs)
        return self

    def add_output(self, name: str):
        self._output_slots[name] = OutputSlot(name, self)
        return self

    def get_input(self, name: str):
        return self._input_slots[name]

    def get_output(self, name: str):
        return self._output_slots[name]

    @property
    def input_slots(self):
        return self._input_slots

    @property
    def output_slots(self):
        return self._output_slots

    def run(self, *args, **kwargs):
        if self._target:
            return self._target(*args, **kwargs)

    # def __rshift__(self, other):
    #     if isinstance(other, Node): # *Node >> Node -> Node
    #         self.OUT.sent(other.IN)
    #         return other
    #     return self.OUT.sent(other) # *Node >> port -> *Node.OUT

    # def __lshift__(self, other):
    #     if isinstance(other, Node): # *Node << Node -> Node
    #         self.IN.recv(other.OUT)
    #         return other
    #     self.IN.recv(other) # *Node << port -> None
    #     return None

    # def __rlshift__(self, other):
    #     self.OUT.sent(other) # port << *Node -> *Node
    #     return self

    # def __rrshift__(self, other):
    #     self.IN.recv(other) # port >> *Node -> *Node
    #     return self


class Const(Node):
    def __init__(self, value: Any):
        super().__init__()
        self._value = value
        self.add_output("value")

    def __repr__(self):
        return f"Const({self._value})"

    def run(self) -> Any:
        return self._value


class Container(Node):
    nodes : Tuple[Node, ...]

    def __len__(self) -> int:
        return len(self.nodes)

    def __getitem__(self, index: int) -> Node:
        return self.nodes[index]


class Sequential(Container):
    def __init__(self, *args: Node):
        super().__init__()
        self.nodes = tuple(args)

        for name, inslots in args[0]._input_slots.items():
            if inslots.has_default():
                self.add_input(name, default=inslots.default)
            else:
                self.add_input(name)

        for name in args[-1].output_slots:
            self.add_output(name)

    def __repr__(self):
        return " >> ".join([repr(n) for n in self.nodes])

    def run(self, *args, **kwargs) -> Any:
        for node in self.nodes:
            args = node.run(*args, **kwargs)
            if not isinstance(args, tuple):
                args = (args,)
            kwargs = {}
        return args


class OutputNode(Node):
    def __init__(self):
        super().__init__()
        self._value = None
        self.add_input("value", default=None)
        self.add_output("out")

    def __repr__(self):
        return "OutputNode"

    def run(self, value):
        self._value = value
        return value

    @property
    def value(self):
        return self._value
