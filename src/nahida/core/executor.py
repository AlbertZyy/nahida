
__all__ = [
    "WorkID",
    "WorkStatus",
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


WorkID = NewType("WorkID", str)

@dataclass(slots=True, frozen=True)
class _WorkItem:
    uid: WorkID
    fn: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
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


class Executor:
    def submit[**P](
        self,
        fn: Callable[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs
    ) -> WorkID:
        """Submits a callable to be executed with the given arguments."""
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
        try:
            result = work_item.fn(*work_item.args, **work_item.kwargs)
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

    def submit[**P](
        self,
        fn: Callable[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs
    ) -> WorkID:
        work_id = WorkID(str(uuid.uuid4()))
        work_item = _WorkItem(work_id, fn, args, kwargs) # TODO: error traceback field
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
        fut = self._futures.pop(work_id)

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
