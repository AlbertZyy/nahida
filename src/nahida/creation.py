
__all__ = ["lambdify_like"]

from collections.abc import Callable

from .core.graph import Graph, ForwardFunc


def lambdify_like(graph: Graph, *, forward: ForwardFunc | None = None):
    def decorator[**P, R](stub: Callable[P, R], /):
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return graph.run(args, kwargs, forward=forward)

        wrapper.__stub__ = stub
        return wrapper

    return decorator
