from py4j.java_gateway import JavaObject

from .jvm import JvmPackage
from .python import classproperty

_jvm_utils = JvmPackage("io.stoys.utils")


class Spark:
    @classproperty
    def rowTypeTag(cls) -> JavaObject:  # noqa: B902,N805
        return _jvm_utils.Spark.rowTypeTag()
