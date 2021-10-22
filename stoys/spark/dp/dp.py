from __future__ import annotations

from py4j.java_gateway import JavaObject
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SQLContext

from ...utils.jvm import JvmPackage
from .dp_config import DpConfig
from .dp_result import DpResult

_jvm_dp = JvmPackage("io.stoys.spark.dp")


class Dp:
    def __init__(self, sql_ctx: SQLContext, java_dp: JavaObject) -> None:
        self._sql_ctx = sql_ctx
        self._java_dp = java_dp

    def config(self, config: DpConfig) -> Dp:
        if config:
            self._java_dp.config(config.to_jvm())
        return self

    def computeDpResult(self) -> SparkDataFrame:
        return SparkDataFrame(self._java_dp.computeDpResult(), self._sql_ctx)

    def dpResult(self) -> DpResult:
        return DpResult.from_jvm(self._java_dp.computeDpResult().first())

    @staticmethod
    def fromDataFrame(sdf: SparkDataFrame) -> Dp:
        return Dp(sdf.sql_ctx, _jvm_dp.Dp.fromDataFrame(sdf._jdf))

    # @staticmethod
    # def fromDataset(sdf: SparkDataFrame) -> Dp:
    #     return Dp(sdf.sql_ctx, _jvm_dp.Dp.fromDataset(sdf._jdf))
