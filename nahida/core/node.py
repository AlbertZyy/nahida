from typing import Tuple, Dict, Mapping, Any, Callable, overload
from collections import OrderedDict
import inspect
from inspect import _ParameterKind as _PK
import time

from .._types import (
    NodeTopologyError,
    InputSlot,
    OutputSlot,
    SlotStatus,
    NodeIOError,
    NodeExceptionData
)

__all__ = [
    "Node",
    "inspected",
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
        return _ConnPortIn(self.input_slots, self)

    @property
    def OUT(self):
        from ..utils import _ConnPortOut
        return _ConnPortOut(self.output_slots, self)


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
            self._input_slots[name] = InputSlot(
                has_default="default" in kwargs,
                default=kwargs.get("default", None)
            )

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
            self._output_slots[name] = OutputSlot()

    def set_param_kinds(self, kinds: Dict[str, _PK]):
        for name, kind in kinds.items():
            if name in self._input_slots:
                input_slot = self._input_slots[name]
                input_slot.param_kind = kind

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
    
    def dump_key(self, slot: str):
        return (self, slot)

    @property
    def input_slots(self):
        return self._input_slots

    @property
    def output_slots(self):
        return self._output_slots

    def run(self, *args, **kwargs):
        if self._target:
            return self._target(*args, **kwargs)

    def execute(self):
        positional_only_data = []
        keyword_data = {}

        try:
            self._execute_impl(positional_only_data, keyword_data)
        except Exception as exception:
            return NodeExceptionData(
                node=self,
                timestamp=time.time(),
                type=type(exception),
                message=str(exception),
                positional_inputs=positional_only_data,
                keyword_inputs=keyword_data
            )

        return None

    def _execute_impl(self, positional_only_data, keyword_data):
        for name, input_slot in self.input_slots.items():
            slot_status = input_slot.status
            if slot_status == SlotStatus.DISABLED:
                continue
            src_node = input_slot.source_node

            if (src_node is None) or (slot_status == SlotStatus.BLOCKED):
                if input_slot.has_default:
                    data = input_slot.default
                else:
                    raise NodeIOError(f"The '{name}' input of node "
                                      f"'{self}' is not connected "
                                       "and not having a default value.")
            else:
                data_box = input_slot.databox
                if data_box is None:
                    raise NodeIOError("Input data box should be assigned before "
                                      "getting the data")
                elif data_box.has_data:
                    data = data_box.data
                else:
                    raise NodeIOError("The input data does not appear, "
                                      "maybe the calculation order is wrong")
                # Release the data box
                input_slot.databox = None

            param_kind = input_slot.param_kind
            if param_kind == _PK.POSITIONAL_ONLY:
                positional_only_data.append(data)
            elif param_kind == _PK.VAR_POSITIONAL:
                assert isinstance(data, tuple)
                positional_only_data.extend(data)
            elif param_kind in (_PK.POSITIONAL_OR_KEYWORD, _PK.KEYWORD_ONLY):
                keyword_data[name] = data
            elif param_kind == _PK.VAR_KEYWORD:
                assert isinstance(data, dict)
                keyword_data.update(data)
            else:
                raise RuntimeError("Inappropriate parameter kind")

        results = self.run(*positional_only_data, **keyword_data)
        if not isinstance(results, tuple):
            results = (results,)

        available_slots = {
            name: slot for name, slot in self.output_slots.items()
            if slot.status != SlotStatus.DISABLED
        }

        for output_slot, data in zip(available_slots.values(), results):
            data_box = output_slot.databox
            if data_box is None:
                raise NodeIOError("Output data box should be generated before "
                                  "putting the data")
            else:
                data_box.data = data
                data_box.has_data = True
            # Release the data box
            output_slot.databox = None


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

def inspected(target: Callable, /, inspector=None, output_name: str = "val"):
    """Initialize a node from callable.

    Args:
        target (Callable): A function specifying the computation flow of a node.
        inspector (Callable | str | None, optional): The object to get the arguments from.
            If it is a string, the attribute with the same name is taken from target.
            If it is None, target itself is used. Defaults to None.
        output_name (str, optional): Name for the output slot.

    Returns:
        node: Node object.
    """
    if inspector is None:
        inspector = target
    elif isinstance(inspector, str):
        inspector = getattr(target, inspector)

    sig = inspect.signature(inspector)
    inputs = []
    defaults = {}
    param_kinds = {}

    for name, param in sig.parameters.items():
        inputs.append(name)
        param_kinds[name] = param.kind

        if param.default is not param.empty:
            defaults[name] = param.default

    node = Node(target, inputs, defaults, (output_name,))
    node.set_param_kinds(param_kinds)
    return node


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
            if inslots.has_default:
                self.register_input(name, default=inslots.default)
            else:
                self.register_input(name)

            self.input_slots[name].param_kind = inslots.param_kind

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
