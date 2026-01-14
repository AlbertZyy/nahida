
from typing import Any, NewType
from collections.abc import Callable
from enum import StrEnum
from dataclasses import dataclass
import traceback
from queue import Queue
import uuid


TaskID = NewType("TaskID", str)

class TaskStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(slots=True, frozen=True)
class ErrorInfo:
    type: str
    message: str
    traceback: str = ""


@dataclass(slots=True, frozen=True)
class TaskEvent:
    task_id: TaskID
    status: TaskStatus
    value: Any | None = None
    error_info: ErrorInfo | None = None


class Executor:
    def submit[**P](
        self,
        fn: Callable[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs
    ) -> TaskID:
        """Submits a callable to be executed with the given arguments."""
        raise NotImplementedError

    def wait(self) -> TaskEvent:
        raise NotImplementedError

    def shutdown(self):
        raise NotImplementedError


class ThreadPoolExecutor(Executor):
    def __init__(self, max_workers: int | None = None) -> None:
        from concurrent.futures import ThreadPoolExecutor as _TPE
        self._event_queue: Queue[TaskEvent] = Queue()
        self._executor = _TPE(max_workers)

    @staticmethod
    def _worker(event_queue: Queue, uid, fn, args, kwargs) -> None:
        try:
            result = fn(*args, **kwargs)
            event = TaskEvent(
                task_id=uid,
                status=TaskStatus.SUCCESS,
                value=result
            )
        except Exception as e:
            event = TaskEvent(
                task_id=uid,
                status=TaskStatus.FAILED,
                error_info=ErrorInfo(
                    e.__class__.__name__,
                    str(e.args[0]) if len(e.args) == 1 else repr(e.args),
                    traceback.format_exc()
                )
            )
        event_queue.put(event)

    def submit[**P](
        self,
        fn: Callable[P, Any],
        /,
        *args: P.args,
        **kwargs: P.kwargs
    ) -> TaskID:
        task_id = TaskID(str(uuid.uuid4()))
        self._executor.submit(
            self._worker, self._event_queue, task_id, fn, args, kwargs
        )
        return task_id

    def wait(self) -> TaskEvent:
        return self._event_queue.get()

    def shutdown(self):
        return self._executor.shutdown()
