
__all__ = [
    "ErrorInfo",
    "ExecEvent",
    "Executor",
    "ThreadPoolExecutor"
]

from typing import Any, NewType
from collections.abc import Callable
from enum import StrEnum
from dataclasses import dataclass, field, asdict
import traceback
from queue import SimpleQueue
import uuid

from .context import Context, DataRef, SimpleDataRef, DataRefFactory
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
    SHUTDOWN = "shutdown"


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

    def is_shutdown(self) -> bool:
        return self.status == TaskStatus.SHUTDOWN


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
        kwargs: dict[str, Expr] = {}
    ) -> TaskID:
        """Submits a task to be executed with the given arguments.

        Args:
            source (int | str): Unique ID of callable registered or source code.
            context (Context): Necessary memories for expressions to evaluate.
            args (tuple of Expr): Expressions for positional arguments.
            kwargs (dict[str, Expr]): Expressions for keyword arguments.

        Returns:
            int: UID of the created work.
        """
        raise NotImplementedError

    def wait(self) -> ExecEvent:
        """Wait for the next work event.

        Returns:
            ExecEvent: an dataclass containing
            - task_id (int | None): The work ID returned by `submit`.
            - status (TaskStatus): success, failed, cancelled or shutdown.
            - value (DataRef | None): Data reference of the returned result.
                Ready for being put back into a context, or directly get the value.
            - error_info (ErrorInfo | None): Error information.
        """
        raise NotImplementedError

    def cancel(self, task_id: TaskID, /) -> bool:
        """Cancels the work with the given task_id."""
        raise NotImplementedError

    def shutdown(self, wait: bool = True) -> None:
        """Try cancel all pending work and shutdown the executor."""
        raise NotImplementedError


class ThreadPoolExecutor(Executor):
    def __init__(self, max_workers: int | None = None, data_ref: DataRefFactory = SimpleDataRef) -> None:
        """A thread pool executor.

        Args:
            max_workers (int | None, optional): Max number of workers.
            data_ref (type): A callable returning a DataRef instance with no
                parameters. DataRef refers to instances that have `get` and
                `set` methods to load and save data.
                Defaults to SimpleDataRef that stores values in memory directly.
        """
        from concurrent.futures import ThreadPoolExecutor as _TPE, Future
        self._event_queue: SimpleQueue[ExecEvent] = SimpleQueue()
        self._executor = _TPE(max_workers)
        self._futures: dict[TaskID, Future] = {}
        self._data_ref_factory = data_ref

    @staticmethod
    def _worker(event_queue: SimpleQueue, task_item: TaskItem, data_ref_factory: DataRefFactory) -> None:
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
                value=data_ref_factory(result)
            )
        except Exception as e:
            event = ExecEvent(
                task_id=task_item.uid,
                status=TaskStatus.FAILED,
                error_info=ErrorInfo(
                    e.__class__.__name__,
                    str(e.args[0]) if len(e.args) == 1 else repr(e.args),
                    traceback.format_exc()# if task_item.error_traceback else ""
                )
            )
            print(event)
        event_queue.put(event)

    def submit(
        self,
        source: int | str,
        context: Context,
        /,
        args: tuple[Expr, ...] = (),
        kwargs: dict[str, Expr] = {}
    ) -> TaskID:
        task_id = TaskID(str(uuid.uuid4()))
        task_item = TaskItem(task_id, source, context, args, kwargs) # TODO: error traceback field
        fut = self._executor.submit(
            self._worker, self._event_queue, task_item, self._data_ref_factory
        )
        self._futures[task_id] = fut
        return task_id

    def wait(self) -> ExecEvent:
        event = self._event_queue.get()

        if event.status in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED):
            if event.task_id is not None:
                self._futures.pop(event.task_id)

        return event

    def cancel(self, task_id: TaskID, /) -> bool:
        try:
            fut = self._futures.pop(task_id)
        except KeyError:
            return False

        if fut.cancel():
            self._event_queue.put(ExecEvent(task_id, TaskStatus.CANCELLED))
            return True
        return False

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

        for task_id in self._futures.keys():
            self._event_queue.put(ExecEvent(task_id, TaskStatus.CANCELLED))

        self._event_queue.put(ExecEvent(None, TaskStatus.SHUTDOWN))

        if wait:
            self._executor.shutdown(wait=True)
