
__all__ = [
    "SubscribedNotFoundError",
    "ExposedNotFoundError",
    "DataGetItemError",
    "ParamMissingError",
    "CircularRecruitmentError",
    "TaskFailedError"
]

from typing import Any


def _make_node_name(node: Any) -> str:
    if hasattr(node, "__name__"):
        return str(node.__name__)
    else:
        return repr(node)


class BaseException(Exception):
    ERROR_CODE = "UNKNOWN"

    def __init__(self, message: str = "", args: tuple[Any, ...] = ()) -> None:
        super().__init__(message)
        self.message = message
        self.args = args

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.ERROR_CODE,
            "message": self.message,
            "args": self.args
        }


class SubscribedNotFoundError(BaseException):
    """Raised when a subscribed data not found in the context."""
    ERROR_CODE = "SCHEDULING_ERROR.SUBSCRIBED_NOTFOUND"
    def __init__(self, node: Any, attr: Any) -> None:
        node_name = _make_node_name(node)
        message = "the upstream node result of attribute {!r} (in {!r}) cannot be found in the context; check execution order".format(node_name, attr)
        super().__init__(message, (node_name, attr))


class ExposedNotFoundError(BaseException):
    """Raised when a exposed data of a graph not found in the context."""
    ERROR_CODE = "SCHEDULING_ERROR.EXPOSED_NOTFOUND"
    def __init__(self, graph: Any, out_port: Any = None) -> None:
        graph_name = _make_node_name(graph)
        if out_port is None:
            message = "the node result (mapping to the output of graph {!r}) cannot be found in the context; check execution order".format(graph_name)
        else:
            message = "the node result (mapping to the output {!r} of graph {!r}) cannot be found in the context; check execution order".format(out_port, graph_name)
        super().__init__(message, (graph_name, out_port))


class DataGetItemError(BaseException):
    """Raised when `KeyError` or `IndexError` is raised in any result subscription."""
    ERROR_CODE = "SCHEDULING_ERROR.GETITEM_FAILED"
    def __init__(self, node: Any, attr: Any, item: Any) -> None:
        node_name = _make_node_name(node)
        message = "the upstream subscribed by {!r} (in {!r}) does not supports getitem by {!r}".format(attr, node_name, item)
        super().__init__(message, (node_name, item))


class ParamMissingError(BaseException):
    """Riased from `Execute` node, when any param required by the internal
    function was not specified (subscription or default value)."""
    ERROR_CODE = "SCHEDULING_ERROR.PARAM_MISSING"
    def __init__(self, node: Any, param: Any) -> None:
        node_name = _make_node_name(node)
        message = "attribute {!r} of {!r} is not set with subscription or value".format(param, node_name)
        super().__init__(message, (node_name, param))


class CircularRecruitmentError(BaseException):
    """Raised when circular recruitment occurs."""
    ERROR_CODE = "SCHEDULING_ERROR.CIRCULAR_RECRUITMENT"
    def __init__(self, node: Any, next_node: Any) -> None:
        node_name = _make_node_name(node)
        next_node_name = _make_node_name(next_node)
        message = "{!r} wants to recruit {!r} that exists in the execution path".format(node_name, next_node_name)
        super().__init__(message, (node_name, next_node_name))


class TaskFailedError(BaseException):
    """Raised when a submitted task raises an exception."""
    ERROR_CODE = "EXECUTION_ERROR.TASK_FAILED"
    def __init__(self, node: Any) -> None:
        node_name = _make_node_name(node)
        message = "node {!r} run failed".format(node_name)
        super().__init__(message, (node_name,))
