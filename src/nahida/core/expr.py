
__all__ = [] # NOTE: not allowed to be imported by *

from typing import Any, TypeGuard
from collections.abc import Callable

from . import errors as _err

type Expr = Callable[[dict[int, Any]], Any]


def is_expr(obj: Any, /) -> TypeGuard[Expr]:
    """Return whether the object is an expression."""
    flag = getattr(obj, "__expression__", False)
    return isinstance(flag, bool) and flag and callable(obj)


def constant(value: Any) -> Expr:
    def wrapped(context: dict[int, Any], /):
        return value
    wrapped.__expression__ = True
    return wrapped


def subscription(
    ctx_id: int,
    index: int | str | None = None,
    *,
    owner: Any = None
) -> Expr:
    def wrapped(context: dict[int, Any], /):
        if ctx_id in context:
            val = context[ctx_id]

            if index is not None:
                try:
                    val = val[index]
                except (KeyError, IndexError) as e:
                    raise _err.DataGetItemError(owner, index) from e

            return val

        raise _err.SubscribedNotFoundError(owner, index)
    wrapped.__expression__ = True

    return wrapped


def formula(
    source: str,
    attributes: dict[str, Expr],
    *,
    owner: Any = None,
    attr_name: Any = None
) -> Expr:
    def wrapped(context: dict[int, Any], /):
        local_vars = {name: func(context) for name, func in attributes.items()}
        try:
            return eval(source, {}, local_vars)
        except Exception as e:
            raise _err.ExprEvalError(owner, attr_name) from e
    wrapped.__expression__ = True

    return wrapped
