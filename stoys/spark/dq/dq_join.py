from __future__ import annotations

from typing import Dict, List

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SQLContext
from pyspark.sql.column import Column

from ...utils.jvm import JvmPackage
from ...utils.py4j import Py4j
from ...utils.spark import Spark
from .dq_config import DqConfig
from .dq_join_result import DqJoinInfo, DqJoinResult
from .dq_join_type import DqJoinType

_jvm_dq = JvmPackage("io.stoys.spark.dq")


class DqJoin:
    def __init__(self, sql_ctx: SQLContext, java_dq_join: JavaObject) -> None:
        self._sql_ctx = sql_ctx
        self._java_dq_join = java_dq_join

    def config(self, config: DqConfig) -> DqJoin:
        if config:
            self._java_dq_join.config(config.to_jvm())
        return self

    def metadata(self, metadata: Dict[str, str]) -> DqJoin:
        if metadata:
            self._java_dq_join.metadata(Py4j.toMap(metadata))
        return self

    def joinType(self, join_type: DqJoinType) -> DqJoin:
        if join_type:
            jvm_join_type = getattr(_jvm_dq.DqJoinType, join_type.value)
            self._java_dq_join.joinType(jvm_join_type)
        return self

    def getDqJoinInfo(self) -> DqJoinInfo:
        return DqJoinInfo.from_jvm(self._java_dq_join.getDqJoinInfo())

    def computeDqJoinStatistics(self) -> SparkDataFrame:
        return SparkDataFrame(self._java_dq_join.computeDqJoinStatistics(), self._sql_ctx)

    def computeDqResult(self) -> SparkDataFrame:
        return SparkDataFrame(self._java_dq_join.computeDqResult(), self._sql_ctx)

    def computeDqJoinResult(self) -> SparkDataFrame:
        return SparkDataFrame(self._java_dq_join.computeDqJoinResult(), self._sql_ctx)

    def dqJoinResult(self) -> DqJoinResult:
        return DqJoinResult.from_jvm(self._java_dq_join.computeDqJoinResult().first())

    @staticmethod
    def equiJoin(
        left_sdf: SparkDataFrame,
        right_sdf: SparkDataFrame,
        left_column_names: List[str],
        right_column_names: List[str],
    ) -> DqJoin:
        java_dq_join = _jvm_dq.DqJoin.equiJoin(
            left_sdf._jdf,
            right_sdf._jdf,
            Py4j.toSeq(left_column_names),
            Py4j.toSeq(right_column_names),
            Spark.rowTypeTag,
            Spark.rowTypeTag,
        )
        return DqJoin(left_sdf.sql_ctx, java_dq_join)

    @staticmethod
    def expensiveArbitraryJoin(left_sdf: SparkDataFrame, right_sdf: SparkDataFrame, join_condition: Column) -> DqJoin:
        java_dq_join = _jvm_dq.DqJoin.expensiveArbitraryJoin(
            left_sdf._jdf,
            right_sdf._jdf,
            join_condition._jc,
            Spark.rowTypeTag,
            Spark.rowTypeTag,
        )
        return DqJoin(left_sdf.sql_ctx, java_dq_join)
