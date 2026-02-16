
__all__ = [
    "branch",
    "expression",
    "formula",
    "gin",
    "graph",
    "nfunc",
    "repeat"
]

from collections.abc import Callable, Iterable, Sequence
from typing import Any

from .chained import NodeChain
from .core.expr import Expr, VariableExpr
from .core.graph import Graph
from .core.node import Node


def branch(
    condition: Expr | bool | None = None,
    true: Node | NodeChain | None = None,
    false: Node | NodeChain | None = None,
):
    """*Control Flow* - Branching execution based on a condition.

    Args:
        condition (Expr | bool, optional): The condition to evaluate.
            Can be an expression or a boolean value.
        true (Node | NodeChain, optional): The node or chain to execute if the
            condition is true.
        false (Node | NodeChain, optional): The node or chain to execute if the
            condition is false.

    Returns:
        Branch: A node that evaluates the condition and executes the
            appropriate branch.
    """
    from .core.node import Branch
    branch_node = Branch(condition)

    if isinstance(true, Node):
        branch_node.true.link(true)

    elif isinstance(true, NodeChain):
        if not isinstance(true.head, Node):
            raise TypeError("Head of the true chain must be a Node.")
        branch_node.true.link(true.head)

    if isinstance(false, Node):
        branch_node.false.link(false)

    elif isinstance(false, NodeChain):
        if not isinstance(false.head, Node):
            raise TypeError("Head of the false chain must be a Node.")
        branch_node.false.link(false.head)

    return branch_node


def expression(func: Callable[..., Any], /):
    """*Data Flow* - A decorator that transforms a function into an expression
    operator.

    Args:
        func (Callable[..., Any]): The function to transform into an expression
            operator.

    Returns:
        Callable: A function that takes the same arguments as `func` but
        returns an expression representing the operation.
    """
    from functools import partial
    from .core.executor import Executor
    from .core.expr import FunctionExpr

    fid = Executor.register(func)
    return partial(FunctionExpr, fid)


def formula(source: str, **kwargs: Expr | Any):
    """*Data Flow* - Create a formula expression from a source string and
    keyword arguments.

    Args:
        source (str): The Python expression representing the formula.
        **kwargs (Expr | Any): Keyword arguments mapping variable names in the
            source string to their corresponding expressions or values.

    Returns:
        FormulaExpr: An formula expression.
    """
    from .core.expr import FormulaExpr, ensure_expr
    return FormulaExpr(
        source,
        **{k: ensure_expr(v) for k, v in kwargs.items()}
    )


gin = VariableExpr(Graph.GRAPH_INPUT_ID)
"""*Data Flow* - A special variable expression representing the graph input."""


def graph(
    entries: Node | NodeChain | Sequence[Node | NodeChain],
    outputs: Expr | tuple[Expr, ...] | dict[str, Expr] | None = None,
):
    """Create a graph from a list of entry nodes or node chains.

    Args:
        entries: Starting nodes or node chains for the graph. Can be a single
            node or node chain, or a sequence of nodes (chains).
        outputs: Expressions representing the outputs of the graph. Can be a
            single expression, a tuple of expressions, or a dictionary mapping
            output names to expressions.
            If None, the graph will not have any explicit outputs.

    Returns:
        Graph: A graph object representing the computation defined by the entry nodes and outputs.
    """
    starters: list[Node] = []
    if not isinstance(entries, Sequence):
        entries = [entries]

    for entry in entries:
        if isinstance(entry, NodeChain):
            if not isinstance(entry.head, Node):
                raise TypeError("Head of the entry chain must be a Node.")
            starters.append(entry.head)
        else:
            starters.append(entry)
    return Graph(starters, exposes=outputs)


def nfunc(func: Callable[..., Any], /):
    """*Node* - A decorator that transform a Python function into an execution node."""
    from .core.executor import Executor
    from .core.node import Execute

    fid = Executor.register(func)
    node = Execute(fid)

    if hasattr(func, "__name__"):
        node.set_name(func.__name__)

    return node


def repeat(
    iterable: Expr | Iterable[Any] | None = None,
    iter: Node | NodeChain | None = None,
    stop: Node | NodeChain | None = None,
):
    """*Control Flow* - Repeat execution based on an iterable.

    Args:
        iterable (Expr | Iterable, optional): The iterable to loop over.
            Can be an expression or a Python iterable.
        iter (Node | NodeChain, optional): The node or chain to execute for each
            iteration. The current iteration value will be available as the
            first element of the Repeat's output.
        stop (Node | NodeChain, optional): The node or chain to execute after
            the iterable exhausts naturally.

    Returns:
        Repeat: A node that executes the loop based on the provided iterable.
    """
    from .core.node import Repeat
    repeat_node = Repeat(iterable)

    if isinstance(iter, Node):
        repeat_node.iter.link(iter)

    elif isinstance(iter, NodeChain):
        if not isinstance(iter.head, Node):
            raise TypeError("Head of the iter chain must be a Node.")
        repeat_node.iter.link(iter.head)

    if isinstance(stop, Node):
        repeat_node.stop.link(stop)

    elif isinstance(stop, NodeChain):
        if not isinstance(stop.head, Node):
            raise TypeError("Head of the stop chain must be a Node.")
        repeat_node.stop.link(stop.head)

    return repeat_node
