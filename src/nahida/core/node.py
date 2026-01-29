# nahida/core/node.py

from __future__ import annotations

__all__ = [
    "FlowControl",
    "OrderItem",
    "Node",
    "Execute",
    "Branch",
    "Repeat",
    "Break",
    "Join"
]

from typing import Any, overload, Literal
from collections.abc import Iterable, Iterator

from . import _objbase
from . import context as _ctx
from . import errors as _err
from . import expr as _expr
from .scheduler import FlowControl, OrderItem


type Expr = _expr.Expr


class Node(_objbase.NameMixin, _expr.RefExpr):
    """Abstract base class for all nodes.

    Nodes are computational units that can be connected to other nodes, and
    are designed with the following interfaces:
      - `order`: order execution/scheduling tasks to the controller,
      - `write`: put output values to the context,

    where `order` is abstract and must be implemented by subclasses, and
    `write` is defaults to putting values into the context dict directly.
    Here `context` is a dictionary of node IDs to the
    corresponding output values.
    """
    def __init__(self, *, uid: int | None = None) -> None:
        _expr.Expr.__init__(self, uid=uid)

    def order(self, context: _ctx.Context) -> OrderItem:
        """Return a task to be submitted to the task queue.

        A task item is a dataclass containing the following fields:
        - target: the function to be executed. None for no execution tasks.
        - args: the positional arguments.
        - kwargs: the keyword arguments.
        - recruit: the nodes to be recruited, also the downstreams.
        - control: the control instruction after execution.

        where the target, args and kwargs represents an exection task, while
        recruit and control for a scheduling task.

        Args:
            context (dict[int, Any]): The context of the environment.

        Returns:
            OrderItem: The task item to be submitted to the task queue.
        """
        raise NotImplementedError

    def exit(self) -> None:
        """Called after any scope that created by this node is exited by
        another node."""
        return


class _ContextReader:
    """Supports read_context in a node.

    Introduce `_values` and `_connects` dicts for defaults and subscriptions.
    """
    _args: list[Expr]
    _kwargs: dict[str, Expr]

    def __init__(self) -> None:
        self._args = []
        self._kwargs = {}

    def subs(self, *args: Expr | Any, **kwargs: Expr | Any) -> None:
        """Set subscriptions for attributes.

        An attribute may take values from other nodes or constants. Use pairs
        like `arg=node` or `arg=value` to define the souce of the attribute
        named `arg`. Use `arg=node[index]` for source nodes returning a
        tuple, list, dict or any other types that supports __getitem__.

        Args:
            *args (Expr | Any): Sources for positional attributes.
            **kwargs (Expr | Any): The attribute-source pairs.
        """
        self._args.extend(
            item if _expr.is_expr(item) else _expr.ConstExpr(item)
            for item in args
        )
        self._kwargs.update(
            {name: item if _expr.is_expr(item) else _expr.ConstExpr(item)
             for name, item in kwargs.items()}
        )

    def unsubs(self, *attrs: int | str) -> None:
        """Remove subscriptions for attributes."""
        for attr in attrs:
            if isinstance(attr, str):
                self._kwargs.pop(attr, None)
            elif isinstance(attr, int):
                try:
                    self._args.pop(attr)
                except IndexError:
                    pass
            else:
                raise TypeError(f"invalid attribute index/key: {attr!r}")

    def numattr(self, kind: Literal["P", "K", None] = None) -> int:
        """Return the number of attribute subscriptions.

        Args:
            kind (Literal["P", "K"] | None): The kind of attributes to count.
                "P" for positional attributes, "K" for keyword attributes,
                None for all attributes. Defaults to all attributes.

        Returns:
            int: The number of attributes.
        """
        if kind == "P":
            return len(self._args)
        elif kind == "K":
            return len(self._kwargs)
        elif kind is None:
            return len(self._args) + len(self._kwargs)
        raise ValueError(f"invalid parameter kind: {kind!r}")

    def keywords(self) -> Iterable[str]:
        """Return an iterable of all the keywords of attributes that having
        subscriptions."""
        return iter(self._kwargs)

    def deps(self) -> set[int]:
        """Return the UIDs of nodes that the attributes depends on."""
        from itertools import chain
        uid_set: set[int] = set()
        for expr in chain(self._args, self._kwargs.values()):
            uid_set |= expr.refs()
        return uid_set

    def read_context(self, context: _ctx.Context, attr: int | str, /) -> tuple[Any, bool]:
        """Try fetching a value for an attribute on the given context."""
        if isinstance(attr, str) and attr in self._kwargs:
            expr = self._kwargs[attr]
        elif isinstance(attr, int) and attr >= -len(self._args) and attr < len(self._args):
            expr = self._args[attr]
        else:
            return None, False

        try:
            return expr.eval(context), True
        except Exception as e:
            raise _err.SubscribeError(self, attr) from e

    def read_context_all_subscriptions(self, context: _ctx.Context, /):
        """Get values for all subscribed attributes on the given context."""
        args: list[Any] = []
        kwargs: dict[str, Any] = {}

        for index, expr in enumerate(self._args):
            try:
                args.append(expr.eval(context))
            except Exception as e:
                raise _err.SubscribeError(self, index) from e

        for key, expr in self._kwargs.items():
            try:
                kwargs[key] = expr.eval(context)
            except Exception as e:
                raise _err.SubscribeError(self, key) from e

        return tuple(args), kwargs


class _Recruiter:
    """Supports routing downstream nodes.

    Introduce `_downstreams` list for downstream nodes.
    `link` and `unlink` are available for manage downstream nodes.
    """
    _downstreams: set[int]

    def __init__(self, downstream: set[int] | None = None, /):
        if downstream is None:
            self._downstreams = set()
        else:
            self._downstreams = set(downstream)

    def link(self, *other: Node) -> None:
        """Add downstream nodes to be recruited after execution.

        Args:
            *other (Node): The downstream nodes to be added.
        """
        self._downstreams.update(node.uid for node in other)

    def linkuid(self, *uid: int) -> None:
        """Add downstream nodes to be recruited after execution.

        Args:
            *uid (int): The UIDs of downstream nodes to be added.
        """
        self._downstreams.update(uid)

    def unlink(self, *other: Node) -> None:
        """Remove downstream nodes. Do nothing for unlinked nodes.

        Args:
            *other (Node): The downstream nodes to be removed.
        """
        self._downstreams.difference_update(node.uid for node in other)

    def unlinkuid(self, *uid: int) -> None:
        """Remove downstream nodes. Do nothing for unlinked UIDs.

        Args:
            *uid (int): The UIDs of downstream nodes to be removed.
        """
        self._downstreams.difference_update(uid)

    def downstream_nodes(self) -> set[Node]:
        """Return downstream nodes."""
        return set(_objbase.get_entity(uid, Node) for uid in self._downstreams)

    @property
    def downstreams(self) -> set[int]:
        """Return downstream node UIDs."""
        return self._downstreams


class Execute(_Recruiter, _ContextReader, Node):
    """Computational Node object."""
    _source: int | str

    def __init__(self, source: int | str, /, *, uid: Any = None):
        Node.__init__(self, uid=uid)
        _ContextReader.__init__(self)
        _Recruiter.__init__(self)
        self._source = source

    def order(self, context: _ctx.Context):
        return OrderItem(
            self.uid,
            source=self._source,
            args=tuple(self._args),
            kwargs=self._kwargs,
            recruit=self.downstream_nodes()
        )


class Branch(_ContextReader, Node):
    """Branch the execution based on a condition."""
    def __init__(self, condition: Expr | bool | None = None, /, *, uid: int | None = None) -> None:
        Node.__init__(self, uid=uid)
        _ContextReader.__init__(self)
        self._downstreams_true = _Recruiter()
        self._downstreams_false = _Recruiter()
        if condition is not None:
            self.subs(condition)

    @property
    def true(self):
        """Execute downstream nodes when condition is True."""
        return self._downstreams_true

    @property
    def false(self):
        """Execute downstream nodes when condition is False."""
        return self._downstreams_false

    def order(self, context: _ctx.Context):
        val, status = self.read_context(context, 0)

        if bool(val) and status:
            return OrderItem(self.uid, recruit=self.true.downstream_nodes())
        else:
            return OrderItem(self.uid, recruit=self.false.downstream_nodes())


class Repeat(_ContextReader, Node):
    """Repeat the execution for multiple times."""
    def __init__(self, iterable: Expr | Iterable[Any] | None = None, *, uid: int | None = None) -> None:
        Node.__init__(self, uid=uid)
        _ContextReader.__init__(self)
        self._downstreams_iter = _Recruiter()
        self._downstreams_stop = _Recruiter()
        if iterable is not None:
            self.subs(iterable)

    @property
    def iter(self):
        """Execute downstream nodes repeatedly."""
        return self._downstreams_iter

    @property
    def stop(self):
        """Execute downstream nodes when the loop is stopped."""
        return self._downstreams_stop

    def order(self, context: _ctx.Context):
        iterable = self.read_context(context, 0)[0]
        repeat_iter = Repeat.Iter(
            self, iter(iterable),
            self.iter.downstream_nodes(), self.stop.downstream_nodes()
        )
        return OrderItem(self.uid, recruit={repeat_iter})

    class Iter(Node):
        def __init__(self, parent: Repeat, iterator: Iterator, ds_iter: set[Node], ds_stop: set[Node]) -> None:
            super().__init__()
            self._parent = parent
            self._ds_iter = ds_iter
            self._ds_stop = ds_stop
            self._iterator = iterator

        def order(self, context: _ctx.Context) -> OrderItem:
            try:
                current = next(self._iterator)
            except StopIteration:
                return OrderItem(self._parent.uid, recruit=self._ds_stop)
            return OrderItem(
                self._parent.uid,
                release=(current,),
                recruit=self._ds_iter,
                control=FlowControl.ENTER,
                recall=self.order
            )

    @overload
    @classmethod
    def from_range(cls, stop: int | Expr = 1, /, *, uid: Any = None) -> Repeat: ...
    @overload
    @classmethod
    def from_range(cls, start: int | Expr, stop: int | Expr, step: int | Expr = 1, /, *, uid: Any = None) -> Repeat: ...
    @classmethod
    def from_range(cls, *args, uid: Any = None) -> Repeat:
        if len(args) == 0:
            start, stop, step = 0, 1, 1
        elif len(args) == 1:
            start, stop, step = 0, args[0], 1
        elif len(args) == 2:
            start, stop, step = args[0], args[1], 1
        elif len(args) == 3:
            start, stop, step = args
        else:
            raise TypeError("Invalid arguments")
        return cls(range(start, stop, step), uid=uid)


class Break(_Recruiter, Node):
    """Break the repeat loop."""
    def order(self, context: _ctx.Context):
        return OrderItem(self.uid, recruit=self.downstream_nodes(), control=FlowControl.EXIT)


class Join(_Recruiter, Node):
    """Block the execution until all receivers are triggered."""
    def __init__(self, num: int = 2, *, uid: Any = None):
        Node.__init__(self, uid=uid)
        _Recruiter.__init__(self)
        self.receivers = tuple(Join.Receiver(self, i) for i in range(num))
        self.flags = [False] * num

    def order(self, context: _ctx.Context) -> OrderItem:
        if all(self.flags):
            self.flags = [False] * len(self.receivers)
            return OrderItem(self.uid, recruit=self.downstream_nodes())
        else:
            return OrderItem(self.uid)

    class Receiver(Node):
        def __init__(self, parent: Join, index: int):
            super().__init__()
            self.parent = parent
            self.index = index

        def order(self, context: _ctx.Context):
            self.parent.flags[self.index] = True
            return OrderItem(self.uid, recruit={self.parent,})
