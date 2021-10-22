from typing import Any, Dict, List

from py4j.java_gateway import JavaObject

from .jvm import JvmPackage
from .python import classproperty

_jvm_utils = JvmPackage("io.stoys.utils")


class Py4j:
    @classproperty
    def emptyOption(cls) -> JavaObject:  # noqa: B902,N805
        return _jvm_utils.Py4j.emptyOption()

    @classproperty
    def emptySeq(cls) -> JavaObject:  # noqa: B902,N805
        return _jvm_utils.Py4j.emptySeq()

    @classproperty
    def emptyMap(cls) -> JavaObject:  # noqa: B902,N805
        return _jvm_utils.Py4j.emptyMap()

    @staticmethod
    def toArray(values: List[Any]) -> JavaObject:
        return _jvm_utils.Py4j.toArray(values)

    @staticmethod
    def toList(values: List[Any]) -> JavaObject:
        return _jvm_utils.Py4j.toList(values)

    @staticmethod
    def toMap(key_values: Dict[Any, Any]) -> JavaObject:
        return _jvm_utils.Py4j.toMap(key_values)

    @staticmethod
    def toOption(value: Any) -> JavaObject:
        return _jvm_utils.Py4j.toOption(value)

    @staticmethod
    def toSeq(values: List[Any]) -> JavaObject:
        return _jvm_utils.Py4j.toSeq(values)

    @staticmethod
    def toSet(values: List[Any]) -> JavaObject:
        return _jvm_utils.Py4j.toSet(values)
