from typing import Union

from py4j.java_gateway import JavaClass, JavaObject

from ..utils.jvm import JvmPackage
from .reflection import Reflection

_jvm_scala = JvmPackage("io.stoys.scala")


class Arbitrary:
    @staticmethod
    def default(java_class: Union[JavaClass, str]) -> JavaObject:
        return _jvm_scala.Arbitrary.default(Reflection._class_name_to_type_tag(java_class))

    @staticmethod
    def empty(java_class: Union[JavaClass, str]) -> JavaObject:
        return _jvm_scala.Arbitrary.empty(Reflection._class_name_to_type_tag(java_class))

    @staticmethod
    def hashed(java_class: Union[JavaClass, str], salt: int) -> JavaObject:
        return _jvm_scala.Arbitrary.hashed(salt, Reflection._class_name_to_type_tag(java_class))

    @staticmethod
    def indexed(java_class: Union[JavaClass, str], salt: int) -> JavaObject:
        return _jvm_scala.Arbitrary.indexed(salt, Reflection._class_name_to_type_tag(java_class))

    @staticmethod
    def proto(java_class: Union[JavaClass, str]) -> JavaObject:
        return _jvm_scala.Arbitrary.proto(Reflection._class_name_to_type_tag(java_class))

    @staticmethod
    def random(java_class: Union[JavaClass, str], seed: int) -> JavaObject:
        return _jvm_scala.Arbitrary.random(seed, Reflection._class_name_to_type_tag(java_class))
