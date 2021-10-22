from __future__ import annotations

from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from ..utils.jvm import JvmPackage
from ..utils.py4j import Py4j
from .reshape import ReshapeConfig
from .spark_io_config import SparkIOConfig
from .table_name import TableName

_jvm_spark = JvmPackage("io.stoys.spark")


class SparkIO:
    def __init__(self, spark_session: SparkSession, config: SparkIOConfig) -> None:
        self._sql_ctx = spark_session._jsc
        self._spark_io = _jvm_spark.SparkIO(spark_session._jsparkSession, config.to_jvm())

    def df(self, entity_name: str, logical_name: Optional[str] = None) -> DataFrame:
        table_name = TableName(entity_name, logical_name)
        return DataFrame(self._spark_io.df(table_name), self._sql_ctx)

    def write(
        self,
        df: DataFrame,
        entity_name: str,
        logical_name: Optional[str] = None,
        format: Optional[str] = None,
        write_mode: Optional[str] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        table_name = TableName(entity_name, logical_name)
        self._spark_io.writeDF(
            df._jdf,
            table_name.fullTableName(),
            Py4j.toOption(format),
            Py4j.toOption(write_mode),
            Py4j.toMap(options or {}),
        )

    def close(self) -> Any:
        return self._spark_io.close()


class SparkIOContext:
    def __init__(
        self,
        spark_session: SparkSession,
        input_paths: List[str],
        output_path: Optional[str],
        write_format: Optional[str] = None,
        write_mode: Optional[str] = None,
        write_options: Optional[Dict[str, str]] = None,
    ) -> None:
        self._spark_session = spark_session
        self._config = SparkIOConfig(
            input_paths,
            ReshapeConfig.default,
            True,
            output_path,
            write_format,
            write_mode,
            write_options or {},
        )

    def __enter__(self) -> SparkIO:
        self._spark_io = SparkIO(self._spark_session, self._config)
        return self._spark_io

    def __exit__(self, exc_type, exc_val, exc_tb) -> Any:  # noqa: ANN001
        return self._spark_io.close()
