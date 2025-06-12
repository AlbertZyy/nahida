from typing import (
    Tuple, Dict, Mapping, Any,
    overload
)
from collections import OrderedDict

from .._types import NodeTopologyError
from .connection import InputSlot, OutputSlot

__all__ = [
    "Node",
    "Const",
    "Sequential"
]


class AbstractNode(object):
    @property
    def input_slots(self) -> Mapping[str, InputSlot]:
        raise NotImplementedError

    @property
    def output_slots(self) -> Mapping[str, OutputSlot]:
        raise NotImplementedError
    
    @property
    def IN(self):
        from ..utils import _ConnPortIn
        return _ConnPortIn(self.input_slots)
    
    @property
    def OUT(self):
        from ..utils import _ConnPortOut
        return _ConnPortOut(self.output_slots)


class Node(AbstractNode):
    _input_slots : OrderedDict[str, InputSlot]
    _output_slots : OrderedDict[str, OutputSlot]

    def __init__(
        self,
        target=None,
        inputs: Tuple[str, ...] = (),
        defaults: Dict[str, Any] = {},
        outputs: Tuple[str, ...] = ()
    ):
        r"""Initialize a compute node."""
        super().__init__()
        self._target = target
        self._input_slots = OrderedDict()
        self._output_slots = OrderedDict()
        self._connection_hooks = OrderedDict()
        self._status_hooks = OrderedDict()

        if target:
            for in_arg in inputs:
                if in_arg in defaults:
                    self.register_input(in_arg, default=defaults[in_arg])
                else:
                    self.register_input(in_arg)

            for out_arg in outputs:
                self.register_output(out_arg)

    @overload
    def register_input(self, name: str) -> None: ...
    @overload
    def register_input(self, name: str, *, default: Any) -> None: ...
    def register_input(self, name: str, **kwargs):
        """Add an input slot to the node."""
        if "_input_slots" not in self.__dict__:
            raise AttributeError(
                "cannot assign inputs before Node.__init__() call"
            )
        elif not isinstance(name, str):
            raise TypeError(
                f"input name should be a string, but got {name.__class__.__name__}"
            )
        elif "." in name:
            raise KeyError('input name can\'t contain "."')
        elif name == "":
            raise KeyError('input name can\'t be empty string ""')
        elif hasattr(self, name) and name not in self._input_slots:
            raise KeyError(f"attribute '{name}' already exists")
        else:
            self._input_slots[name] = InputSlot(**kwargs)

    def register_output(self, name: str) -> None:
        """Add an output slot to the node."""
        if "_output_slots" not in self.__dict__:
            raise AttributeError(
                "cannot assign outputs before Node.__init__() call"
            )
        elif not isinstance(name, str):
            raise TypeError(
                f"output name should be a string, but got {name.__class__.__name__}"
            )
        elif "." in name:
            raise KeyError('output name can\'t contain "."')
        elif name == "":
            raise KeyError('output name can\'t be empty string ""')
        elif hasattr(self, name) and name not in self._output_slots:
            raise KeyError(f"attribute '{name}' already exists")
        else:
            self._output_slots[name] = OutputSlot(self)

    def get_input(self, name: str):
        """Return the input slot given by `name`. Raises NodeTopologyError if not exists."""
        if name not in self.input_slots:
            raise NodeTopologyError(f"no input named {name}")
        return self._input_slots[name]

    def get_output(self, name: str):
        """Return the output slot given by `name`. Raises NodeTopologyError if not exists."""
        if name not in self.output_slots:
            raise NodeTopologyError(f"no output named {name}")
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
        self.register_output("value")

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

        for name, inslots in args[0].input_slots.items():
            if inslots.has_default():
                self.register_input(name, default=inslots.get_value())
            else:
                self.register_input(name)

        for name in args[-1].output_slots:
            self.register_output(name)

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
        self.register_input("val", default=None)
        self.register_output("out")

    def __repr__(self):
        return "OutputNode"

    def run(self, val):
        self._value = val
        return val

    @property
    def value(self):
        return self._value
