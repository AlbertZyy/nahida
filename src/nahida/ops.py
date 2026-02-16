
__all__ = ["nfunc_factory", "nprint"]

from typing import Any, Callable

from .core.node import Execute
from .creation import nfunc


def nfunc_factory(func: Callable[..., Any]) -> Callable[..., Execute]:
    from .core.executor import Executor
    Executor.register(func)

    def _node_factory(*args, **kwargs):
        node = nfunc(func)
        node.subs(*args, **kwargs)
        return node
    return _node_factory


nprint = nfunc_factory(print)
"""*Node* - Create a node that executes the built-in print function
with the provided arguments/expressions."""
