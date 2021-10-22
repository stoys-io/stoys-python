from __future__ import annotations

from typing import List

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession

from ...utils.jvm import JvmPackage
from ...utils.py4j import Py4j
from .aggsum_config import AggSumConfig
from .aggsum_result import AggSumInfo, AggSumResult

_jvm_aggsum = JvmPackage("io.stoys.spark.aggsum")


class AggSum:
    def __init__(self, sql_ctx: SQLContext, java_aggsum: JavaObject) -> None:
        self._sql_ctx = sql_ctx
        self._java_aggsum = java_aggsum

    def config(self, config: AggSumConfig) -> AggSum:
        if config:
            self._java_aggsum.config(config.to_jvm())
        return self

    def keyColumnNames(self, key_column_names: List[str]) -> AggSum:
        if key_column_names:
            self._java_aggsum.keyColumnNames(Py4j.toSeq(key_column_names))
        return self

    def referencedTableNames(self, referenced_table_names: List[str]) -> AggSum:
        if referenced_table_names:
            self._java_aggsum.referencedTableNames(Py4j.toSeq(referenced_table_names))
        return self

    def getAggSumInfo(self) -> AggSumInfo:
        return AggSumInfo.from_jvm(self._java_aggsum.getAggSumInfo())

    def computeAggSum(self) -> SparkDataFrame:
        return SparkDataFrame(self._java_aggsum.computeAggSum(), self._sql_ctx)

    def computeAggSumResult(self) -> SparkDataFrame:
        return SparkDataFrame(self._java_aggsum.computeAggSumResult(), self._sql_ctx)

    def aggSumResult(self) -> AggSumResult:
        return AggSumResult.from_jvm(self._java_aggsum.computeAggSumResult().first())

    @staticmethod
    def fromDataFrame(sdf: SparkDataFrame) -> AggSum:
        return AggSum(sdf.sql_ctx, _jvm_aggsum.AggSum.fromDataFrame(sdf._jdf))

    # @staticmethod
    # def fromDataset(sdf: SparkDataFrame) -> AggSum:
    #     return AggSum(sdf.sql_ctx, _jvm_aggsum.AggSum.fromDataset(sdf._jdf))

    @staticmethod
    def fromSql(spark_session: SparkSession, sql: str) -> AggSum:
        sql_ctx = SQLContext(spark_session.sparkContext)
        return AggSum(sql_ctx, _jvm_aggsum.AggSum.fromSql(spark_session._jsparkSession, sql))
