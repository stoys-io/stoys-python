from __future__ import annotations

from typing import Any, Optional

from .python import classproperty


def Option(value: Any) -> Optional[Any]:
    return value


def Some(value: Any) -> Optional[Any]:
    return value


class Seq(list):
    def __init__(self, *args: Any) -> None:
        list.__init__(self, list(args))

    @classproperty
    def empty(cls) -> Seq[Any]:  # noqa: B902,N805
        return Seq()


class Map(dict):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # if len(args) > 0 and isinstance(args[0], tuple):
        # dict.__init__(self, *args, **kwargs)
        dict.__init__(self, {**dict(list(args)), **dict(kwargs)})

    @classproperty
    def empty(cls) -> Map[Any]:  # noqa: B902,N805
        return Map()
