from __future__ import annotations

from typing import Any


class DataRef:
    empty = object()
    _value: Any

    def __init__(self, value: Any = empty, /) -> None:
        self._value = value

    def get(self) -> Any:
        if self._value is DataRef.empty:
            raise ValueError()
        return self._value

    def set(self, value: Any) -> None:
        self._value = value


class Context:
    _data: dict[int, DataRef]

    def __init__(self, data_ref_type: type[DataRef] = DataRef):
        self._data = {}
        self._ref_type = data_ref_type

    def view(self, uids: set[int]) -> Context:
        ctx = Context(self._ref_type)
        for index in uids:
            try:
                ctx._data[index] = self._data[index]
            except KeyError:
                pass
        return ctx

    def read(self, uid: int, /, index: Any = None) -> Any:
        ref = self._data[uid]
        val = ref.get()

        if index is not None:
            val = val[index]

        return val

    def write(self, uid: int, value: Any, /) -> None:
        ref = self._ref_type(value)
        self._data[uid] = ref

    def get_all(self) -> dict[int, Any]:
        return {key: ref.get() for key, ref in self._data.items()}

    def dump(self) -> dict[int, str]:
        raise NotImplementedError()

    def load(self, data: dict[int, str]):
        raise NotImplementedError()
