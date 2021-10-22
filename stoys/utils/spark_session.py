from py4j.java_gateway import JavaClass
from pyspark.sql import SparkSession

from ..config import STOYS_LIB_MIN_JAVA_VERSION

_is_spark_session_builder_patched: bool = False


def _patch_spark_session_builder() -> None:
    global _is_spark_session_builder_patched
    if not _is_spark_session_builder_patched:
        from ..deps.use import add_stoys_jars

        original_spark_session_builder_get_or_create = SparkSession.Builder.getOrCreate
        SparkSession.Builder.getOrCreate = lambda b: original_spark_session_builder_get_or_create(add_stoys_jars(b))
        _is_spark_session_builder_patched = True


def get_or_create_spark_session() -> SparkSession:
    spark_sesion = SparkSession.getActiveSession()
    if spark_sesion is None:
        _patch_spark_session_builder()
        spark_sesion = SparkSession.builder.getOrCreate()
        check_spark_session_with_stoys_library()
    return spark_sesion


def check_spark_session_with_stoys_library() -> None:
    spark_sesion = SparkSession.getActiveSession()
    if spark_sesion is None:
        raise Exception("SparkSession.getActiveSession() is None. Have you created SparkSession yet?")
    jvm_view = spark_sesion._jvm
    java_version = jvm_view.java.lang.System.getProperty("java.version")
    if java_version < STOYS_LIB_MIN_JAVA_VERSION:
        raise Exception(
            f"""Current Java version is {java_version}.
                Minimum required Java version is {STOYS_LIB_MIN_JAVA_VERSION}.
                Consider setting JAVA_HOME to newer Java before starting SparkSession."""  # noqa: C812
        )
    if not isinstance(jvm_view.io.stoys.scala.Jackson, JavaClass):
        # TODO: Show nice instructions how to add our jars to the environment.
        #       Note: This is very different for different environments. jupyterlab / databricks / zeppelin
        raise Exception("TODO: Missing stoys jars! Please add them to spark and restart SparkSession.")
