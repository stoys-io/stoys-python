from dataclasses import dataclass, replace
from typing import ClassVar, Type, TypeVar

import serde

# from dataclasses_json import dataclass_json
from py4j.java_gateway import JavaClass, JavaObject

from ..scala import Jackson
from .json import Json
from .jvm import Jvm
from .python import classproperty

CCM = TypeVar("CCM", bound="CaseClassMirror")


class CaseClassMirror(object):
    _stoys_: ClassVar[str]
    _java_class: ClassVar[JavaClass] = None

    @classproperty
    def java_class(cls: Type[CCM]) -> JavaClass:  # noqa: B902,N805
        if cls._java_class is None:
            cls._java_class = Jvm.java_class(cls._stoys_)
        return cls._java_class

    def copy(self: CCM, **kwargs) -> CCM:  # noqa: ANN003
        return replace(self, **kwargs)

    def to_json(self: CCM) -> str:
        return Json.to_json(self)

    @classmethod
    def from_json(cls: Type[CCM], json: str) -> CCM:
        return Json.from_json(cls, json)

    def to_jvm(self: CCM) -> JavaObject:
        return Jackson.fromJson(self.java_class, self.to_json())

    @classmethod
    def from_jvm(cls: Type[CCM], java_object: JavaObject) -> CCM:
        return cls.from_json(Jackson.toJson(java_object))
        # return cls.from_json(Jackson.toJson(java_object), infer_missing=True)


def case_class_mirror(jvm_class_name: str) -> CCM:
    def wrapper(cls: Type[CCM]) -> CCM:
        cls = serde.deserialize()(serde.serialize()(dataclass(frozen=True)(cls)))
        # cls = dataclass_json()(dataclass(frozen=True)(cls))
        cls._stoys_ = jvm_class_name
        return cls

    return wrapper
