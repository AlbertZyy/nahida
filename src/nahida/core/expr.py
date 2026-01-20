from __future__ import annotations

__all__ = [] # NOTE: not allowed to be imported by *

from typing import Any, TypeGuard
from collections.abc import Callable, Mapping

from . import _objbase
from . import errors as _err


def _to_expr(obj: Any, /) -> Expr:
    if isinstance(obj, Expr):
        return obj
    return ConstExpr(obj)


class Expr(_objbase.UIDMixin):
    """Base class for expressions

    Expressions are designed for lightweight data transformations between nodes.
    They are evaluated in a stack-like manner.
    """
    def eval(self, context: Mapping[int, Any], /) -> Any:
        """Evaluate the expression on the given context."""
        raise NotImplementedError()

    def __call__(self, context: Mapping[int, Any], /) -> Any:
        return self.eval(context)

    def __getitem__(self, index: int | str) -> GetItemExpr:
        return GetItemExpr(self, index)

    def __or__(self, other: Any, /) -> UnionExpr:
        return UnionExpr(self, _to_expr(other))

    def __ror__(self, other: Any, /) -> UnionExpr:
        return UnionExpr(_to_expr(other), self)


def is_expr(obj: Any, /) -> TypeGuard[Expr]:
    """Return whether the object is an expression."""
    return isinstance(obj, Expr)


class simple_fetcher(Expr):
    def __init__(self, index: int) -> None:
        super().__init__()
        self._obj = index

    def eval(self, context: Mapping[int, Any], /) -> Any:
        try:
            return context[self._obj]
        except KeyError as e:
            raise _err.DataNotFoundError(self._obj) from e


class ConstExpr[T](Expr):
    """Constant expression that always returns the given value."""
    def __init__(self, value: T, /) -> None:
        super().__init__()
        self._value = value

    def eval(self, context: Mapping[int, Any], /) -> T:
        return self._value


class RefExpr(Expr):
    """Reference expression that subscribes values from context.

    Raises *DataNotFoundError* if the UID of this expression does not exist in
    the context.
    """
    def eval(self, context: Mapping[int, Any], /) -> Any:
        try:
            return context[self.uid]
        except KeyError as e:
            raise _err.DataNotFoundError(self) from e


class GetItemExpr(Expr):
    """Get-item expression that leverages the `__getitem__` method of another
    expression's value.

    Raises *DataGetItemError* when failed.
    """
    def __init__(self, expr: Expr, index: int | str, /) -> None:
        super().__init__()
        self._expr = expr
        self._index = index

    def eval(self, context: Mapping[int, Any], /) -> Any:
        val = self._expr.eval(context)

        try:
            val = val[self._index]
        except (KeyError, IndexError) as e:
            raise _err.DataGetItemError(type(val).__name__, self._index) from e

        return val


class UnionExpr(Expr):
    """Union expression that returns the first value that was successfully
    evaluated.

    Raises *UnionError* after all child expression failed.
    """
    def __init__(self, *exprs: Expr) -> None:
        super().__init__()
        self._exprs: list[Expr] = []

        for expr in exprs:
            if isinstance(expr, UnionExpr):
                self._exprs.extend(expr._exprs)
            else:
                self._exprs.append(expr)

    def eval(self, context: Mapping[int, Any], /) -> Any:
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


class FormulaExpr(Expr):
    """Formula expression that evaluates a Python expression.

    Raises *ExprEvalError* when the evaluation failed.

    Args:
        source (str): Python expression.
        **attributes (dict[str, Expr]): values for variables in the source.
    """
    def __init__(self, source: str, /, **locals: Expr) -> None:
        save_builtins = dict(__builtins__).copy()
        save_builtins.update(vars(__import__("math")))
        save_builtins.pop("__import__", None)
        save_builtins.pop("__loader__", None)

        self._source = source
        self._func = lambda **kwargs: eval(
            source, {"__builtins__": save_builtins}, kwargs
        )
        self._locals = locals

    def eval(self, context: Mapping[int, Any], /) -> Any:
        local_vars = {name: value.eval(context) for name, value in self._locals.items()}
        try:
            return self._func(**local_vars)
        except Exception as e:
            raise _err.ExprEvalError() from e


class FunctionExpr(Expr):
    """Function expression that returns the result of a function call.

    Raises *ExprEvalError* when the evaluation failed.
    """
    def __init__(self, func: Callable[..., Any], /, *args: Expr, **kwargs: Expr) -> None:
        super().__init__()
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def eval(self, context: Mapping[int, Any], /) -> Any:
        local_args = [arg.eval(context) for arg in self._args]
        local_kwargs = {name: value.eval(context) for name, value in self._kwargs.items()}
        try:
            return self._func(*local_args, **local_kwargs)
        except Exception as e:
            raise _err.ExprEvalError() from e


def expression(func: Callable[..., Any], /):
    """A decorator that transforms a function into an expression operator."""
    from functools import partial
    return partial(FunctionExpr, func)
