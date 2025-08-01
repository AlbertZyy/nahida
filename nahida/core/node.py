from typing import Any, overload
from collections.abc import Callable
from collections import OrderedDict
import inspect
from inspect import _ParameterKind as _PK
from functools import partial

from . import edge as _E
from ._types import InputSlot, OutputSlot
from ._types import ParamPassingKind as PPK

__all__ = [
    "Node",
    "inspected",
    "Const",
    "Sequential"
]


class Node():
    _input_slots : OrderedDict[str, InputSlot]
    _output_slots : OrderedDict[str, OutputSlot]

    def __init__(self, target=None, variable: bool = False):
        r"""Initialize a compute node."""
        super().__init__()
        self._target = target
        self._variable = variable
        self._input_slots = OrderedDict()
        self._output_slots = OrderedDict()
        self._connection_hooks = OrderedDict()
        self._status_hooks = OrderedDict()

    def __bool__(self) -> bool:
        return True

    def register_input(
            self,
            name: str,
            variable: bool = False,
            parameter: str | int | None = None,
            **kwargs
    ):
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
            if isinstance(parameter, int):
                param_name, param_kind = name, parameter
            else:
                if parameter is None:
                    parameter = name
                param_name, param_kind = parse_signature(parameter)

            self._input_slots[name] = InputSlot(
                has_default="default" in kwargs,
                default=kwargs.get("default", None),
                param_name=param_name,
                param_kind=param_kind,
                variable=variable
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

    def set_param_kinds(self, kinds: dict[str, _PK]):
        for name, kind in kinds.items():
            if name in self._input_slots:
                input_slot = self._input_slots[name]
                input_slot.param_kind = kind

    # def get_input(self, name: str):
    #     """Return the input slot given by `name`. Raises NodeTopologyError if not exists."""
    #     if name not in self._input_slots:
    #         raise NodeTopologyError(f"no input named {name}")
    #     return self._input_slots[name]

    # def get_output(self, name: str):
    #     """Return the output slot given by `name`. Raises NodeTopologyError if not exists."""
    #     if name not in self._output_slots:
    #         raise NodeTopologyError(f"no output named {name}")
    #     return self._output_slots[name]

    def dump_key(self, slot: str):
        return (self, slot)

    @property
    def input_slots(self):
        return self._input_slots

    @property
    def output_slots(self):
        return self._output_slots

    @property
    def is_variable(self):
        return self._variable

    def run(self, *args, **kwargs):
        if self._target:
            return self._target(*args, **kwargs)

    def __call__(self, **kwargs: _E.AddrHandler):
        if self._variable:
            for name in kwargs.keys():
                if name not in self.input_slots:
                    self.register_input(name, variable=False)
        _E.connect_from_address(self.input_slots, kwargs)
        return _E.AddrHandler(self, None)


def inspected(
        target: Callable | None,
        /,
        inspector: Callable | str | None = None,
        var_slot: bool = True,
        var_node: bool = True,
        outputs: str | tuple[str, ...] = "out"
):
    """Initialize a node from callable.

    Args:
        target (Callable): A function specifying the computation flow of a node.
        inspector (Callable | str | None, optional): The object to get the arguments from.
            If it is a string, the attribute with the same name is taken from target.
            If it is None, target itself is used. Defaults to None.
        outputs (str | Tuple[str, ...], optional): Name for the output slot.

    Returns:
        node: Node object.
    """
    if inspector is None:
        inspector = target
    elif isinstance(inspector, str):
        inspector = getattr(target, inspector)
    if isinstance(outputs, str):
        outputs = (outputs,)

    sig = inspect.signature(inspector)
    node = Node(target)

    for name, param in sig.parameters.items():
        param.annotation
        kwargs = {"name": name, "parameter": param.kind}

        if var_slot and param.kind == _PK.VAR_POSITIONAL:
            kwargs["variable"] = True

        if var_node and param.kind == _PK.VAR_KEYWORD:
            node._variable = True

        if param.default is not param.empty:
            kwargs["default"] = param.default

        node.register_input(**kwargs)

    for name in outputs:
        node.register_output(name)

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
    nodes : tuple[Node, ...]

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


class VariableOutputs(Node):
    def __init__(self, data: dict[str, Any], /):
        super().__init__(variable=True)
        self.data = data

    def run(self, **kwargs):
        self.data.update(kwargs)


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


def parse_signature(parameter: str) -> tuple[str | None, PPK]:
    """Get the name and kind of the parameter used by the node to call the function.

    Args:
        parameter (str): The parameter of a parameter.
            - `'/'` - positional
            - `'*'` - variable positional
            - `'{name}'` - keyword
            - `'**'` - variable keyword
    """
    parameter = parameter.strip()

    if parameter == "/":
        return None, PPK.POSITIONAL
    elif parameter == "*":
        return None, PPK.VAR_POSITIONAL
    elif parameter == "**":
        return None, PPK.VAR_KEYWORD
    else:
        return parameter, PPK.KEYWORD
