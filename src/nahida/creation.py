
__all__ = ["expression", "nodal"]

from typing import Any
from collections.abc import Callable


def expression(func: Callable[..., Any], /):
    """A decorator that transforms a function into an expression operator."""
    from functools import partial
    from .core.executor import Executor
    from .core.expr import FunctionExpr

    fid = Executor.register(func)
    return partial(FunctionExpr, fid)


def nodal(func: Callable[..., Any], /):
    """A decorator that transform a function into an execution node."""
    from .core.executor import Executor
    from .core.node import Execute

    fid = Executor.register(func)
    return Execute(fid, uid=fid)
