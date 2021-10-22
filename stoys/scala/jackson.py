from typing import ClassVar

from py4j.java_gateway import JavaClass, JavaObject, get_java_class

from ..utils.jvm import JvmPackage
from ..utils.python import classproperty

_jvm_scala = JvmPackage("io.stoys.scala")


class Jackson:
    _json: ClassVar[JavaObject] = None

    @classproperty
    def json(cls) -> JavaClass:  # noqa: B902,N805
        if cls._json is None:
            cls._json = _jvm_scala.Jackson.json()
        return cls._json

    @staticmethod
    def toJson(java_object: JavaObject) -> str:
        return Jackson.json.writeValueAsString(java_object)

    @staticmethod
    def fromJson(java_class: JavaClass, json: str) -> JavaObject:
        return Jackson.json.readValue(json, get_java_class(java_class))
