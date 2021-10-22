from __future__ import annotations

from typing import Dict, List

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession, SQLContext

from ...utils.jvm import JvmPackage
from ...utils.py4j import Py4j
from .dq_config import DqConfig
from .dq_field import DqField
from .dq_result import DqResult
from .dq_rule import DqRule

_jvm_dq = JvmPackage("io.stoys.spark.dq")


class Dq:
    def __init__(self, sql_ctx: SQLContext, java_dq: JavaObject) -> None:
        self._sql_ctx = sql_ctx
        self._java_dq = java_dq

    def config(self, config: DqConfig) -> Dq:
        if config:
            self._java_dq.config(config.to_jvm())
        return self

    def fields(self, fields: List[DqField]) -> Dq:
        if fields:
            self._java_dq.fields(Py4j.toSeq([f.to_jvm() for f in fields]))
        return self

    def metadata(self, metadata: Dict[str, str]) -> Dq:
        if metadata:
            self._java_dq.metadata(Py4j.toMap(metadata))
        return self

    def primaryKeyFieldNames(self, primary_key_field_names: List[str]) -> Dq:
        if primary_key_field_names:
            self._java_dq.primaryKeyFieldNames(Py4j.toSeq(primary_key_field_names))
        return self

    def rules(self, rules: List[DqRule]) -> Dq:
        if rules:
            self._java_dq.rules(Py4j.toSeq([r.to_jvm() for r in rules]))
        return self

    def computeDqResult(self) -> SparkDataFrame:
        return SparkDataFrame(self._java_dq.computeDqResult(), self._sql_ctx)

    def computeDqViolationPerRow(self) -> SparkDataFrame:
        return SparkDataFrame(self._java_dq.computeDqViolationPerRow(), self._sql_ctx)

    def selectFailingRows(self) -> SparkDataFrame:
        return SparkDataFrame(self._java_dq.selectFailingRows(), self._sql_ctx)

    def selectPassingRows(self) -> SparkDataFrame:
        return SparkDataFrame(self._java_dq.selectPassingRows(), self._sql_ctx)

    def dqResult(self) -> DqResult:
        return DqResult.from_jvm(self._java_dq.computeDqResult().first())

    @staticmethod
    def fromDataFrame(sdf: SparkDataFrame) -> Dq:
        return Dq(sdf.sql_ctx, _jvm_dq.Dq.fromDataFrame(sdf._jdf))

    # @staticmethod
    # def fromDataset(sdf: SparkDataFrame) -> Dq:
    #     return Dq(sdf.sql_ctx, jvm_dq.Dq.fromDataset(sdf._jdf))

    @staticmethod
    def fromDqSql(spark_session: SparkSession, dq_sql: str) -> Dq:
        sql_ctx = SQLContext(spark_session.sparkContext)
        return Dq(sql_ctx, _jvm_dq.Dq.fromDqSql(spark_session._jsparkSession, dq_sql))

    @staticmethod
    def fromFileInputPath(spark_session: SparkSession, input_path: str) -> Dq:
        sql_ctx = SQLContext(spark_session.sparkContext)
        return Dq(
            sql_ctx,
            _jvm_dq.Dq.fromFileInputPath(spark_session._jsparkSession, input_path),
        )

    @staticmethod
    def fromTableName(spark_session: SparkSession, table_name: str) -> Dq:
        sql_ctx = SQLContext(spark_session.sparkContext)
        return Dq(sql_ctx, _jvm_dq.Dq.fromTableName(spark_session._jsparkSession, table_name))
