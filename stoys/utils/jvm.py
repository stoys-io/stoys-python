from __future__ import annotations

from typing import List, Optional, Union

from py4j.java_gateway import JavaClass, JavaPackage, JVMView
from pyspark import SparkContext
from pyspark.sql import SparkSession

from .spark_session import get_or_create_spark_session


class JvmPackage:
    def __init__(
        self,
        package_name: Optional[str] = None,
        *,
        context: Optional[Union[JVMView, SparkContext, SparkSession]] = None,
    ) -> None:
        self._package_name = package_name
        self._class_cache = {}
        self._jvm_view = None
        if context is not None:
            if isinstance(context, JVMView):
                self._jvm_view = context
            elif isinstance(context, SparkSession):
                self._jvm_view = context._jvm
            elif isinstance(context, SparkContext):
                self._jvm_view = context._jvm
            else:
                raise Exception(f"Unsupported context type ({type(context)}).")
            self._check_stoys_installed()

    def __dir__(self) -> List[str]:
        return [*self.__dict__, *self._class_cache]

    def __getattr__(self, name: str) -> Union[JavaClass, JvmPackage]:
        if name not in self._class_cache:
            full_name = name if self._package_name is None else f"{self._package_name}.{name}"
            java_class_or_package = getattr(self._jvm, full_name)
            if isinstance(java_class_or_package, JavaPackage):
                self._class_cache[name] = JvmPackage(java_class_or_package.fqn)
            else:
                self._class_cache[name] = java_class_or_package
        return self._class_cache[name]

    def __call__(self, name: str) -> Union[JavaClass, JvmPackage]:
        return self.__getattr__(name)

    @property
    def _jvm(self) -> JVMView:
        if self._jvm_view is None:
            self._jvm_view = get_or_create_spark_session()._jvm
        return self._jvm_view


_jvm_root = JvmPackage()


class Jvm:
    @classmethod
    def java_class(cls, name: str) -> JavaClass:  # noqa: ANN102
        java_class = getattr(_jvm_root, name)
        if isinstance(java_class, JavaClass):
            return java_class
        else:
            raise Exception(f"Class {name} not found in JVM.") from None
