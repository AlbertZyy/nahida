
__all__ = ["PullPort", "PushPort"]

from typing import overload, Any, Self
from collections.abc import Callable
from dataclasses import dataclass, field

from ._types import ParamPassingKind as PPK


class _PortDescBase[T, V, R]:
    __ports__ = ("__name__", "factory", "proxy")

    def __init__(self, factory: Callable[[T], V], proxy: Callable[[T, Self, V], R]):
        self.__name__ = None
        self.factory = factory
        self.proxy = proxy

    def __set_name__(self, owner: type[T], name: str):
        if name != self.__name__:
            raise ValueError(
                "ports cannot be assigned to two different names: "
                f"{name!r} and {self.__name__!r}"
            )

        self.__name__ = name

    def __get__(self, instance: T, owner: type[T]):
        if instance is None:
            return self

        if not hasattr(instance, "__dict__"):
            raise TypeError

        if self.__name__ in instance.__dict__:
            rt = instance.__dict__[self.__name__]
        else:
            rt = self.factory(instance)
            instance.__dict__[self.__name__] = rt

        return self.proxy(instance, self, rt)

    def __set__(self, instance: T, value):
        raise ValueError("cannot assign to ports")


class PullPort[T](_PortDescBase[T, "PullPortRuntime[T]", "PullPortProxy[T]"]):
    def __init__(self, param: str | None = None):
        super().__init__(lambda _: PullPortRuntime(), PullPortProxy)
        self.param = param


@dataclass(slots=True)
class PullPortRuntime[T]:
    has_default: bool = False
    default: Any | None = None
    sources: list[int] = field(default_factory=list, init=False, compare=False)
    upstreams: list[T] = field(default_factory=list, init=False, compare=False)


class PullPortProxy[T]:
    def __init__(self, instance: T, build: PullPort[T], runtime: PullPortRuntime[T]):
        self._instance = instance
        self._build = build
        self._runtime = runtime

    @property
    def __name__(self):
        return self._build.__name__

    @property
    def id(self):
        return id(self._runtime)

    def subscribe(self, port: "PushPortProxy[T]" | int):
        if isinstance(port, int):
            self._runtime.sources.append(port)
        elif isinstance(port, PushPortProxy):
            self._runtime.sources.append(port.id)
            self._runtime.upstreams.append(port._instance)

    def get_default(self) -> T | None:
        return self._runtime.default

    def set_default(self, value: T) -> None:
        self._runtime.has_default = True
        self._runtime.default = value

    def del_default(self) -> None:
        self._runtime.default = None
        self._runtime.has_default = False

    def sources(self):
        yield from self._runtime.sources

    def upstreams(self):
        yield from self._runtime.upstreams


class PushPort[T](_PortDescBase[T, "PushPortRuntime[T]", "PushPortProxy[T]"]):
    def __init__(self):
        super().__init__(lambda _: PushPortRuntime(), PushPortProxy)


@dataclass(slots=True)
class PushPortRuntime[T]:
    pass


class PushPortProxy[T]:
    def __init__(self, instance: T, build: PushPort[T], runtime: PushPortRuntime[T]):
        self._instance = instance
        self._build = build
        self._runtime = runtime

    @property
    def __name__(self):
        return self._build.__name__

    @property
    def id(self):
        return id(self._runtime)
