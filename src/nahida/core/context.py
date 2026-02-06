from __future__ import annotations

__all__ = ["DataRef", "SimpleDataRef", "Context"]

from collections.abc import Callable
from typing import Any, Protocol, Self


empty = object()


class DataRef(Protocol):
    def get(self, item: Any = empty, /) -> Any:
        """Get value from the data reference."""
    def set(self, value: Any) -> None:
        """Set value to the data reference."""

type DataRefFactory = Callable[..., DataRef]


class SimpleDataRef:
    _value: Any

    def __init__(self, value: Any = empty, /) -> None:
        self._value = value

    def get(self, item: Any = empty, /) -> Any:
        if self._value is empty:
            raise ValueError()
        if item is not empty:
            return self._value[item]
        return self._value

    def set(self, value: Any) -> None:
        self._value = value


class Context:
    _data: dict[int, DataRef]
    _trace: tuple[int, ...]

    def __init__(
        self,
        data: dict[int, DataRef] | None = None,
        trace: tuple[int, ...] | None = None,
        *,
        data_ref_factory: DataRefFactory = SimpleDataRef
    ) -> None:
        self._data = data if (data is not None) else {}
        self._trace = trace if (trace is not None) else ()
        self._data_ref_factory = data_ref_factory

    def __getitem__(self, uid: int, /) -> DataRef:
        return self._data[uid]

    def __setitem__(self, uid: int, ref: DataRef, /) -> None:
        self._data[uid] = ref

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def new(self, value: Any = empty, /):
        return self._data_ref_factory(value)

    def view(self, uids: set[int], /) -> Context:
        ctx = Context()
        for index in uids:
            try:
                ctx._data[index] = self._data[index]
            except KeyError:
                pass
        return ctx

    def mark(self, uid: int, /) -> None:
        self._trace = self._trace + (uid,)

    def fork(self) -> Self:
        _self_type = type(self)
        return _self_type(self._data.copy(), self._trace)

    def dump(self) -> dict[int, str]:
        raise NotImplementedError()

    def load(self, data: dict[int, str]):
        raise NotImplementedError()
