from typing import Union

from py4j.java_gateway import JavaClass, JavaObject, get_java_class

from ..utils.jvm import JvmPackage
from ..utils.py4j import Py4j

_jvm_scala = JvmPackage("io.stoys.scala")


class Reflection:
    @staticmethod
    def copy_case_class(case_class_instance: JavaObject, **kwargs) -> JavaObject:  # noqa: ANN003
        return _jvm_scala.Reflection.copyCaseClass(case_class_instance, Py4j.toMap(kwargs))

    @staticmethod
    def _class_name_to_type_tag(java_class: Union[JavaClass, str]) -> JavaObject:
        if isinstance(java_class, JavaClass):
            java_class_name = get_java_class(java_class).getName()
        elif isinstance(java_class, str):
            java_class_name = java_class
        else:
            raise Exception(f"Cannot get java class name from object of type {type(java_class)}") from None
        return _jvm_scala.Reflection.classNameToTypeTag(java_class_name)
