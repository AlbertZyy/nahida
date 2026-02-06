
__all__ = [
    "ErrorInfo",
    "ExecEvent",
    "Executor",
    "ThreadPoolExecutor"
]

from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, field, asdict
from enum import StrEnum
from typing import Any, NewType

from .context import Context, DataRef
from .expr import Expr


TaskID = NewType("TaskID", str)

@dataclass(slots=True, frozen=True)
class TaskItem:
    uid: TaskID
    source: int | str
    context: Context
    args: tuple[Expr, ...] = field(default_factory=tuple)
    kwargs: dict[str, Expr] = field(default_factory=dict)
    error_traceback: bool = False


class TaskStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(slots=True, frozen=True)
class ErrorInfo:
    type: str
    message: str
    traceback: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True, frozen=True)
class ExecEvent:
    task_id: TaskID | None
    status: TaskStatus
    value: DataRef | None = None
    error_info: ErrorInfo | None = None

    def is_success(self) -> bool:
        return self.status == TaskStatus.SUCCESS

    def is_failed(self) -> bool:
        return self.status == TaskStatus.FAILED

    def is_cancelled(self) -> bool:
        return self.status == TaskStatus.CANCELLED


class Executor:
    _callable_registry: dict[int, Callable[..., Any]] = {}

    @classmethod
    def register(cls, target: Callable[..., Any], *, fid: int | None = None) -> int:
        if fid is None:
            if hasattr(target, "__name__"):
                fid = hash(target.__name__)
                while fid in cls._callable_registry:
                    fid += 1
            else:
                raise ValueError()

        elif fid in cls._callable_registry:
            raise KeyError(f"id {fid} already exist")

        cls._callable_registry[fid] = target

        return fid

    def submit(
        self,
        source: int | str,
        context: Context,
        /,
        args: tuple[Expr, ...] = (),
        kwargs: dict[str, Expr] = {},
        callback: Callable[[ExecEvent], Any] | None = None
    ) -> TaskID:
        """Submits a task to be executed with the given arguments.

        Args:
            source (int | str): Unique ID of registered callable or source code.
            context (Context): Necessary memories for expressions to evaluate.
            args (tuple of Expr): Expressions for positional arguments.
            kwargs (dict[str, Expr]): Expressions for keyword arguments.
            callback ((ExecEvent) -> Any, optional): A function called when
                the task done.

        Returns:
            int: ID of the created task.
        """
        raise NotImplementedError

    def cancel(self, task_id: TaskID, /) -> bool:
        """Cancels the task with the given ID."""
        raise NotImplementedError

    def shutdown(self, wait: bool = True) -> None:
        """Try cancel all pending work and shutdown the executor."""
        raise NotImplementedError


class ThreadPoolExecutor(Executor):
    def __init__(self, max_workers: int | None = None) -> None:
        """A thread pool executor.

        Args:
            max_workers (int | None, optional): Max number of workers.
        """
        from concurrent.futures import ThreadPoolExecutor as _TPE
        self._executor = _TPE(max_workers)
        self._futures: dict[TaskID, tuple[Future[None], Future[ExecEvent]]] = {}

    @staticmethod
    def _worker(task_item: TaskItem, event_fut: Future[ExecEvent]) -> None:
        if event_fut.done():
            return

        fid = task_item.source
        context = task_item.context
        try:
            if isinstance(fid, int):
                fn = Executor._callable_registry[fid]
                args = [expr.eval(context) for expr in task_item.args]
                kwargs = {key: expr.eval(context) for key, expr in task_item.kwargs.items()}
                result = fn(*args, **kwargs)
            elif isinstance(fid, str):
                fn = lambda kwargs: exec(fid, kwargs)
                kwargs = {key: expr.eval(context) for key, expr in task_item.kwargs.items()}
                result = fn(**kwargs)
            else:
                raise TypeError(f"invalid type of work item source: {type(fid).__name__}")

            event = ExecEvent(
                task_id=task_item.uid,
                status=TaskStatus.SUCCESS,
                value=context.new(result)
            )
        except Exception as e:
            import traceback
            event = ExecEvent(
                task_id=task_item.uid,
                status=TaskStatus.FAILED,
                error_info=ErrorInfo(
                    e.__class__.__name__,
                    str(e.args[0]) if len(e.args) == 1 else repr(e.args),
                    traceback.format_exc() if task_item.error_traceback else ""
                )
            )
        if not event_fut.done():
            event_fut.set_result(event)

    def submit(
        self,
        source: int | str,
        context: Context,
        /,
        args: tuple[Expr, ...] = (),
        kwargs: dict[str, Expr] = {},
        callback: Callable[[ExecEvent], Any] | None = None
    ) -> TaskID:
        import uuid
        from concurrent.futures import Future
        task_id = TaskID(str(uuid.uuid4()))
        task_item = TaskItem(task_id, source, context, args, kwargs) # TODO: error traceback field
        event_fut: Future[ExecEvent] = Future()
        fut = self._executor.submit(self._worker, task_item, event_fut)
        if callback is not None:
            event_fut.add_done_callback(lambda fut: callback(fut.result()))
        self._futures[task_id] = (fut, event_fut)
        return task_id

    def cancel(self, task_id: TaskID, /) -> None:
        try:
            fut, efut = self._futures.pop(task_id)
        except KeyError:
            return

        fut.cancel()

        if not efut.done():
            efut.set_result(ExecEvent(task_id, TaskStatus.CANCELLED))

    def shutdown(self, wait: bool = True) -> None:
        for task_id in tuple(self._futures.keys()):
            self.cancel(task_id)

        if wait:
            self._executor.shutdown(wait=True)
