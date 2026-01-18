from __future__ import annotations

__all__ = [] # NOTE: not allowed to be imported by *

from typing import Any, TypeGuard
from collections.abc import Callable

from . import errors as _err


def _to_expr(obj: Any, /) -> Expr:
    if not is_expr(obj):
        obj = constant(obj)

    return obj


def _general_eval(obj: Any, /, context: dict[int, Any]) -> Any:
    if is_expr(obj):
        return obj.eval(context)
    else:
        return obj


class Expr:
    """Callable on context to get values."""
    def eval(self, context: dict[int, Any], /) -> Any:
        """Evaluate the expression on the given context."""
        raise NotImplementedError()

    def __getitem__(self, index: int | str) -> subscription:
        return subscription(self, index)

    def __or__(self, other: Any, /) -> union:
        return union(self, _to_expr(other))

    def __ror__(self, other: Any, /) -> union:
        return union(_to_expr(other), self)


def is_expr(obj: Any, /) -> TypeGuard[Expr]:
    """Return whether the object is an expression."""
    return isinstance(obj, Expr)


class simple_fetcher(Expr):
    def __init__(self, index: int) -> None:
        super().__init__()
        self._obj = index

    def eval(self, context: dict[int, Any], /) -> Any:
        try:
            return context[self._obj]
        except KeyError as e:
            raise _err.DataNotFoundError(self._obj) from e


class constant(Expr):
    def __init__(self, value: Any, /) -> None:
        """Construct a constant expression."""
        super().__init__()
        self._value = value

    def eval(self, context: dict[int, Any], /):
        return self._value


class subscription(Expr):
    def __init__(
        self,
        expr: Expr,
        index: int | str,
    ) -> None:
        """Construct an expression subscribing values from context.

        The expression raises:
            - *DataGetItemError*: when `data[index]` failed.
        """
        super().__init__()
        self._expr = expr
        self._index = index

    def eval(self, context: dict[int, Any], /):
        val = self._expr.eval(context)

        try:
            val = val[self._index]
        except (KeyError, IndexError) as e:
            raise _err.DataGetItemError(type(val).__name__, self._index) from e

        return val


class union(Expr):
    def __init__(self, *exprs: Expr) -> None:
        """Construct an expression returning the first value that was successfully
        evaluated.

        The expression raises:
            - *UnionError*: after all failed.
        """
        super().__init__()
        self._exprs: list[Expr] = []

        for expr in exprs:
            if isinstance(expr, union):
                self._exprs.extend(expr._exprs)
            else:
                self._exprs.append(expr)

    def eval(self, context: dict[int, Any], /):
        for expr in self._exprs:
            try:
                return expr.eval(context)
            except (
                _err.DataNotFoundError,
                _err.DataGetItemError,
                _err.ExprEvalError
            ):
                continue

        raise _err.UnionError()


class formula(Expr):
    def __init__(
        self,
        source: str,
        **attributes: Expr | Any,
    ) -> None:
        """Construct an expression of the given formula.

        The expression raises:
            - *ExprEvalError*: when evaluation failed.

        Args:
            source (str): Python expression.
            **attributes (dict[str, Expr]): values for variables in the source.
        """
        self._func = lambda **kwargs: eval(source, {}, kwargs)
        self._attributes = attributes

    def eval(self, context: dict[int, Any], /):
        local_vars: dict[str, Any] = {}
        for name, value in self._attributes.items():
            local_vars[name] = _general_eval(value, context)
        try:
            return self._func(**local_vars)
        except Exception as e:
            raise _err.ExprEvalError() from e


class functional(Expr):
    def __init__(self, func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> None:
        super().__init__()
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def eval(self, context: dict[int, Any], /):
        local_args = [_general_eval(arg, context) for arg in self._args]
        local_kwargs = {name: _general_eval(value, context) for name, value in self._kwargs.items()}
        return self._func(*local_args, **local_kwargs)


def expression(func: Callable[..., Any], /):
    """A decorator that transforms a function into an expression operator."""
    from functools import partial
    return partial(functional, func)
