from typing import Literal
from dataclasses import dataclass, field

from nahida.core.node import Node


Real = int | float
Prim = bool | int | float | str | None


@dataclass(slots=True)
class SlotType:
    name        : str
    dtype       : str | int          = 0
    ttype       : Literal[0, 1, 2]   = 1
    desc        : str                = ""
    title       : str | None         = None
    group       : str | None         = None
    param       : str | None         = None
    default     : Prim               = field(default=None, kw_only=True)
    min_val     : Real | None        = field(default=None, kw_only=True)
    max_val     : Real | None        = field(default=None, kw_only=True)
    step        : Real | None        = field(default=None, kw_only=True)
    max_len     : int | None         = field(default=None, kw_only=True)
    items       : list[Prim]         = field(default_factory=list, kw_only=True)

    def __post_init__(self) -> None:
        if self.ttype not in (0, 1, 2):
            raise ValueError("ttype must be 0, 1 or 2")

        if self.title is None:
            self.title = self.name

        if self.param is None:
            self.param = self.name


class NodeType:
    TITLE: str
    PATH: str = ""
    INPUT_SLOTS: list[SlotType] = []
    OUTPUT_SLOTS: list[SlotType] = []
    VARIABLE: bool = False
    DEPENDENCY: dict[str, set[str]] = {}

    REGISTRY: dict[str, type["NodeType"]] = {}

    def __init_subclass__(cls):
        super().__init_subclass__()
        if cls.__name__ not in cls.REGISTRY:
            cls.REGISTRY[cls.__name__] = cls
        else:
            raise ValueError(f"NodeType {cls.__name__} already exists")

    def __new__(cls):
        node = Node(getattr(cls, "run"), cls.VARIABLE)

        for data in cls.INPUT_SLOTS:
            node.register_input(
                name=data.name,
                variable=data.ttype == 2,
                parameter=data.param,
                default=data.default
            )

        for data in cls.OUTPUT_SLOTS:
            node.register_output(name=data.name)

        return node

    @classmethod
    def types(cls):
        return cls.REGISTRY.values()


def search(name: str, /):
    if name in NodeType.REGISTRY:
        return NodeType.REGISTRY[name]
    else:
        return None


def create(name: str, /):
    if name in NodeType.REGISTRY:
        return NodeType.REGISTRY[name]()
    else:
        raise ValueError(f"NodeType {name} not found")
