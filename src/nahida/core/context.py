from __future__ import annotations

__all__ = ["DataRef", "Context"]

from typing import Any


class DataRef:
    empty = object()
    _value: Any

    def __init__(self, value: Any = empty, /) -> None:
        self._value = value

    def get(self, item: Any = empty, /) -> Any:
        if self._value is DataRef.empty:
            raise ValueError()
        if item is not DataRef.empty:
            return self._value[item]
        return self._value

    def set(self, value: Any) -> None:
        self._value = value


class Context:
    _data: dict[int, DataRef]

    def __init__(self):
        self._data = {}

    def __getitem__(self, uid: int, /) -> DataRef:
        return self._data[uid]

    def __setitem__(self, uid: int, ref: DataRef, /) -> None:
        self._data[uid] = ref

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def view(self, uids: set[int], /) -> Context:
        ctx = Context()
        for index in uids:
            try:
                ctx._data[index] = self._data[index]
            except KeyError:
                pass
        return ctx

    def dump(self) -> dict[int, str]:
        raise NotImplementedError()

    def load(self, data: dict[int, str]):
        raise NotImplementedError()
