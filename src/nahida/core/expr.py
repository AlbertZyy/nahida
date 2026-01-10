
__all__ = [] # NOTE: not allowed to be imported by *

from typing import Any, TypeGuard
from collections.abc import Callable

from . import errors as _err

type Expr = Callable[[dict[int, Any]], Any]


def is_expr(obj: Any, /) -> TypeGuard[Expr]:
    """Return whether the object is an expression."""
    flag = getattr(obj, "__expression__", False)
    return isinstance(flag, bool) and flag and callable(obj)


def constant(value: Any, /) -> Expr:
    """Construct a constant expression."""
    def expr_func(context: dict[int, Any], /):
        return value
    expr_func.__expression__ = True
    return expr_func


def subscription(
    ctx_id: int,
    index: int | str | None = None,
) -> Expr:
    """Construct an expression subscribing values from context.

    The expression raises:
        - *DataNotFoundError*: when `ctx_id` was not in context;
        - *DataGetItemError*: when `data[index]` failed.
    """
    def expr_func(context: dict[int, Any], /):
        if ctx_id in context:
            val = context[ctx_id]

            if index is not None:
                try:
                    val = val[index]
                except (KeyError, IndexError) as e:
                    raise _err.DataGetItemError(ctx_id, index) from e

            return val

        raise _err.DataNotFoundError(ctx_id)
    expr_func.__expression__ = True

    return expr_func


def union(*exprs: Expr) -> Expr:
    """Construct an expression returning the first value that was successfully
    evaluated.

    The expression raises:
        - *UnionError*: after all failed.
    """
    def expr_func(context: dict[int, Any], /):
        for expr in exprs:
            try:
                return expr(context)
            except (
                _err.DataNotFoundError,
                _err.DataGetItemError,
                _err.ExprEvalError
            ):
                continue

        raise _err.UnionError()
    expr_func.__expression__ = True

    return expr_func


def formula(
    source: str,
    attributes: dict[str, Expr],
) -> Expr:
    """Construct an expression of the given formula.

    The expression raises:
        - *ExprEvalError*: when evaluation failed.

    Args:
        source (str): Python expression.
        attributes (dict[str, Expr]): values for variables in the source.
    """
    def expr_func(context: dict[int, Any], /):
        local_vars = {name: func(context) for name, func in attributes.items()}
        try:
            return eval(source, {}, local_vars)
        except Exception as e:
            raise _err.ExprEvalError() from e
    expr_func.__expression__ = True

    return expr_func
