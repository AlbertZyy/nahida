
__all__ = [
    "DataNotFoundError",
    "DataGetItemError",
    "UnionError",
    "ExprEvalError",
    "SubscribeError",
    "ExposingError",
    "ParamMissingError",
    "CircularRecruitmentError",
    "TaskFailedError"
]

from typing import Any
from ._objbase import get_entity, has_entity


def _make_node_name(node: Any) -> str:
    if has_entity(node):
        return str(get_entity(node))
    else:
        return repr(node)


class NBaseException(Exception):
    ERROR_CODE = "UNKNOWN"

    def __init__(self, message: str = "", context: tuple[Any, ...] = ()) -> None:
        super().__init__(message)
        self.message = message
        self.context = context

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.ERROR_CODE,
            "message": self.message,
            "context": self.context
        }


## In expressions

class DataNotFoundError(NBaseException):
    """Raised when a subscribed data not found in the context."""
    ERROR_CODE = "SCHEDULING_ERROR.DATA_NOTFOUND"
    def __init__(self, obj: Any) -> None:
        name = _make_node_name(obj)
        message = "key {} not found in the context; check execution order".format(name)
        super().__init__(message, (name,))


class DataGetItemError(NBaseException):
    """Raised when `KeyError` or `IndexError` is raised in any result subscription."""
    ERROR_CODE = "SCHEDULING_ERROR.DATA_GETITEM_FAILED"
    def __init__(self, type: str, output_item: Any) -> None:
        message = "data of type {!r} does not supports getitem by {!r}".format(type, output_item)
        super().__init__(message, (type, output_item))


class UnionError(NBaseException):
    """Raised when failed to evaluate any union member expression."""
    ERROR_CODE = "SCHEDULING_ERROR.UNION_FAILED"
    def __init__(self) -> None:
        message = "failed to evaluate any of the union members"
        super().__init__(message, ())


class ExprEvalError(NBaseException):
    """Raised when expression evaluation failed."""
    ERROR_CODE = "SCHEDULING_ERROR.EXPRESSION_FAILED"
    def __init__(self) -> None:
        message = "failed to evaluate the expression"
        super().__init__(message, ())


## In nodes/graphs

class SubscribeError(NBaseException):
    """Raised when a node failed to fetch the subscribed data at runtime."""
    ERROR_CODE = "SCHEDULING_ERROR.SUBSCRIPTION_FAILED"
    def __init__(self, node: Any, attr_name: Any) -> None:
        node_name = _make_node_name(node)
        message = "failed to fetch data for attribute {!r} of {!r}".format(attr_name, node_name)
        super().__init__(message, (node_name, attr_name))


class ExposingError(NBaseException):
    """Raised when a exposed data of a graph not found in the context."""
    ERROR_CODE = "SCHEDULING_ERROR.EXPOSED_NOTFOUND"
    def __init__(self, graph: Any, expose_item: Any = None) -> None:
        graph_name = _make_node_name(graph)
        if expose_item is None:
            message = "the result mapping to the output of graph {!r} cannot be found in the context; check execution order".format(graph_name)
        else:
            message = "the result mapping to the output {!r} of graph {!r} cannot be found in the context; check execution order".format(expose_item, graph_name)
        super().__init__(message, (graph_name, expose_item))


class ParamMissingError(NBaseException):
    """Riased from `Execute` node, when any param required by the internal
    function was not specified (subscription or default value)."""
    ERROR_CODE = "SCHEDULING_ERROR.PARAM_MISSING"
    def __init__(self, node: Any, param: Any) -> None:
        node_name = _make_node_name(node)
        message = "attribute {!r} of {!r} is not set with subscription or value".format(param, node_name)
        super().__init__(message, (node_name, param))


class CircularRecruitmentError(NBaseException):
    """Raised when circular recruitment occurs."""
    ERROR_CODE = "SCHEDULING_ERROR.CIRCULAR_RECRUITMENT"
    def __init__(self, node: Any, next_node: Any) -> None:
        node_name = _make_node_name(node)
        next_node_name = _make_node_name(next_node)
        message = "{!r} wants to recruit {!r} that exists in the execution path".format(node_name, next_node_name)
        super().__init__(message, (node_name, next_node_name))


class TaskFailedError(NBaseException):
    """Raised when a submitted task raises an exception."""
    ERROR_CODE = "EXECUTION_ERROR.TASK_FAILED"
    def __init__(self, node: Any) -> None:
        node_name = _make_node_name(node)
        message = "node {!r} run failed".format(node_name)
        super().__init__(message, (node_name,))
