from __future__ import annotations

__all__ = [] # NOTE: not allowed to be imported by *

from typing import Any, TypeGuard

from . import errors as _err
from ._objbase import UIDMixin
from .context import Context


def ensure_expr(obj: Any, /) -> Expr:
    if isinstance(obj, Expr):
        return obj
    return ConstExpr(obj)


class Expr(UIDMixin):
    """Base class for expressions

    Expressions are designed for lightweight data transformations between nodes.
    They are evaluated in a stack-like manner.
    """
    def eval(self, context: Context, /) -> Any:
        """Evaluate the expression on the given context."""
        raise NotImplementedError()

    def refs(self) -> set[int]:
        """Return the UIDs of all RefExprs that this expression depends on."""
        return set()

    def __getitem__(self, index: int | str, /) -> GetItemExpr:
        return GetItemExpr(self, index)

    def __or__(self, other: Any, /) -> UnionExpr:
        return UnionExpr(self, ensure_expr(other))

    def __ror__(self, other: Any, /) -> UnionExpr:
        return UnionExpr(ensure_expr(other), self)


def is_expr(obj: Any, /) -> TypeGuard[Expr]:
    """Return whether the object is an expression."""
    return isinstance(obj, Expr)


class VariableExpr(Expr):
    def __init__(self, target_uid: int, /) -> None:
        super().__init__()
        self._target_uid = target_uid

    def __getitem__(self, index: int | str, /) -> VariableGetItemExpr:
        return VariableGetItemExpr(self._target_uid, index)

    def eval(self, context: Context, /) -> Any:
        try:
            return context[self._target_uid].get()
        except KeyError as e:
            raise _err.DataNotFoundError(self._target_uid) from e

    def refs(self) -> set[int]:
        return {self._target_uid}


class VariableGetItemExpr(Expr):
    def __init__(self, target_uid: int, index: Any, /) -> None:
        super().__init__()
        self._target_uid = target_uid
        self._index = ensure_expr(index)

    def eval(self, context: Context, /) -> Any:
        try:
            index = self._index.eval(context)
            return context[self._target_uid].get(index)
        except KeyError as e:
            raise _err.DataNotFoundError(self._target_uid) from e

    def refs(self) -> set[int]:
        return {self._target_uid} | self._index.refs()


class ConstExpr[T](Expr):
    """Constant expression that always returns the given value."""
    def __init__(self, value: T, /) -> None:
        super().__init__()
        self._value = value

    def eval(self, context: Context, /) -> T:
        return self._value


class RefExpr(Expr):
    """Reference expression that subscribes values from context.

    Raises *DataNotFoundError* if the UID of this expression does not exist in
    the context.
    """
    def __getitem__(self, index: int | str, /) -> VariableGetItemExpr:
        return VariableGetItemExpr(self.uid, index)

    def eval(self, context: Context, /) -> Any:
        try:
            return context[self.uid].get()
        except KeyError as e:
            raise _err.DataNotFoundError(self.uid) from e

    def refs(self) -> set[int]:
        return {self.uid}


class GetItemExpr(Expr):
    """Get-item expression that leverages the `__getitem__` method of another
    expression's value.

    Raises *DataGetItemError* when failed.
    """
    def __init__(self, expr: Expr, index: int | str | Expr, /) -> None:
        super().__init__()
        self._expr = expr
        self._index = ensure_expr(index)

    def eval(self, context: Context, /) -> Any:
        val = self._expr.eval(context)
        index = self._index.eval(context)

        try:
            val = val[index]
        except Exception as e:
            raise _err.DataGetItemError(type(val).__name__, index) from e

        return val

    def refs(self) -> set[int]:
        return self._expr.refs() | self._index.refs()


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

    def eval(self, context: Context, /) -> Any:
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

    def refs(self) -> set[int]:
        result: set[int] = set()

        for expr in self._exprs:
            result |= expr.refs()

        return result


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

    def eval(self, context: Context, /) -> Any:
        local_vars = {name: value.eval(context) for name, value in self._locals.items()}
        try:
            return self._func(**local_vars)
        except Exception as e:
            raise _err.ExprEvalError() from e

    def refs(self) -> set[int]:
        result: set[int] = set()

        for loc in self._locals.values():
            result |= loc.refs()

        return result


class FunctionExpr(Expr):
    """Function expression that returns the result of a function call.

    Raises *ExprEvalError* when the evaluation failed.
    """
    def __init__(self, fid: int, /, *args: Expr, **kwargs: Expr) -> None:
        self._fid = fid
        self._args = args
        self._kwargs = kwargs

    def eval(self, context: Context, /) -> Any:
        local_args = [arg.eval(context) for arg in self._args]
        local_kwargs = {name: value.eval(context) for name, value in self._kwargs.items()}
        try:
            from .executor import Executor
            fn = Executor._callable_registry[self._fid]
            return fn(*local_args, **local_kwargs)
        except Exception as e:
            raise _err.ExprEvalError() from e

    def refs(self) -> set[int]:
        result: set[int] = set()

        for arg in self._args:
            result |= arg.refs()

        for kwarg in self._kwargs.values():
            result |= kwarg.refs()

        return result
