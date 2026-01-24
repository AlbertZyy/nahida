
__all__ = [
    "WorkID",
    "ErrorInfo",
    "WorkEvent",
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

from .context import Context
from .expr import Expr


WorkID = NewType("WorkID", int)

@dataclass(slots=True, frozen=True)
class _WorkItem:
    uid: WorkID
    source: int | str
    context: Context
    args: tuple[Expr, ...] = field(default_factory=tuple)
    kwargs: dict[str, Expr] = field(default_factory=dict)
    error_traceback: bool = False


class WorkStatus(StrEnum):
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
class WorkEvent:
    work_id: WorkID | None
    status: WorkStatus
    value: Any | None = None
    error_info: ErrorInfo | None = None

    def is_success(self) -> bool:
        return self.status == WorkStatus.SUCCESS

    def is_failed(self) -> bool:
        return self.status == WorkStatus.FAILED

    def is_cancelled(self) -> bool:
        return self.status == WorkStatus.CANCELLED

    def is_shutdown(self) -> bool:
        return self.status == WorkStatus.SHUTDOWN


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
        *args: Expr,
        **kwargs: Expr
    ) -> WorkID:
        """Submits a task to be executed with the given arguments.

        Args:
            source (int | str): Unique ID of callable registered or source code.
            context (Context): Necessary memories for expressions to evaluate.
            *args (Expr): Expressions for positional arguments.
            **kwargs (Expr): Expressions for keyword arguments.

        Returns:
            int: UID of the created work.
        """
        raise NotImplementedError

    def wait(self) -> WorkEvent:
        """Wait for the next work event."""
        raise NotImplementedError

    def cancel(self, work_id: WorkID, /) -> bool:
        """Cancels the work with the given work_id."""
        raise NotImplementedError

    def shutdown(self, wait: bool = True) -> None:
        """Try cancel all pending work and shutdown the executor."""
        raise NotImplementedError


class ThreadPoolExecutor(Executor):
    def __init__(self, max_workers: int | None = None) -> None:
        from concurrent.futures import ThreadPoolExecutor as _TPE, Future
        self._event_queue: SimpleQueue[WorkEvent] = SimpleQueue()
        self._executor = _TPE(max_workers)
        self._futures: dict[WorkID, Future] = {}

    @staticmethod
    def _worker(event_queue: SimpleQueue, work_item: _WorkItem) -> None:
        fid = work_item.source
        context = work_item.context
        try:
            if isinstance(fid, int):
                fn = Executor._callable_registry[fid]
                args = [expr.eval(context) for expr in work_item.args]
                kwargs = {key: expr.eval(context) for key, expr in work_item.kwargs.items()}
                result = fn(*args, **kwargs)
            elif isinstance(fid, str):
                fn = lambda kwargs: exec(fid, kwargs)
                kwargs = {key: expr.eval(context) for key, expr in work_item.kwargs.items()}
                result = fn(**kwargs)
            else:
                raise TypeError(f"invalid type of work item source: {type(fid).__name__}")

            event = WorkEvent(
                work_id=work_item.uid,
                status=WorkStatus.SUCCESS,
                value=result
            )
        except Exception as e:
            event = WorkEvent(
                work_id=work_item.uid,
                status=WorkStatus.FAILED,
                error_info=ErrorInfo(
                    e.__class__.__name__,
                    str(e.args[0]) if len(e.args) == 1 else repr(e.args),
                    traceback.format_exc() if work_item.error_traceback else ""
                )
            )
        event_queue.put(event)

    def submit(
        self,
        source: int | str,
        context: Context,
        /,
        *args: Expr,
        **kwargs: Expr
    ) -> WorkID:
        work_id = WorkID(int(uuid.uuid4()))
        work_item = _WorkItem(work_id, source, context, args, kwargs) # TODO: error traceback field
        fut = self._executor.submit(
            self._worker, self._event_queue, work_item
        )
        self._futures[work_id] = fut
        return work_id

    def wait(self) -> WorkEvent:
        event = self._event_queue.get()

        if event.status in (WorkStatus.SUCCESS, WorkStatus.FAILED, WorkStatus.CANCELLED):
            if event.work_id is not None:
                self._futures.pop(event.work_id)

        return event

    def cancel(self, work_id: WorkID, /) -> bool:
        try:
            fut = self._futures.pop(work_id)
        except KeyError:
            return False

        if fut.cancel():
            self._event_queue.put(WorkEvent(work_id, WorkStatus.CANCELLED))
            return True
        return False

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

        for work_id in self._futures.keys():
            self._event_queue.put(WorkEvent(work_id, WorkStatus.CANCELLED))

        self._event_queue.put(WorkEvent(None, WorkStatus.SHUTDOWN))

        if wait:
            self._executor.shutdown(wait=True)
