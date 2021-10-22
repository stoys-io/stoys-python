from typing import Dict, List, Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import DataType

from ..spark.aggsum import AggSum, AggSumConfig, AggSumResult
from ..spark.dp import Dp, DpConfig, DpResult
from ..spark.dq import Dq, DqConfig, DqField, DqJoin, DqJoinResult, DqResult, DqRule
from ..spark.reshape import Reshape
from ..spark.reshape_config import ReshapeConfig


# TODO: Can we remove the duplication here and StoysApiPandas with some super class?
class PysparkDataFrameExtension:
    def __init__(self, sdf: SparkDataFrame) -> None:
        self._sdf = sdf

    def reshape(
        self,
        target_schema: DataType,
        config: Optional[ReshapeConfig] = None,
    ) -> SparkDataFrame:
        return Reshape.reshapeToDF(self._sdf, target_schema, config)

    def aggsum(
        self,
        key_column_names: Optional[List[str]] = None,
        referenced_table_names: Optional[List[str]] = None,
        config: Optional[AggSumConfig] = None,
    ) -> AggSumResult:
        aggsum = AggSum.fromDataFrame(self._sdf).config(config)
        aggsum.keyColumnNames(key_column_names).referencedTableNames(referenced_table_names)
        return aggsum.aggSumResult()

    def dp(
        self,
        config: Optional[DpConfig] = None,
    ) -> DpResult:
        dp = Dp.fromDataFrame(self._sdf).config(config)
        return dp.dpResult()

    def dq(
        self,
        rules: Optional[List[DqRule]] = None,
        fields: Optional[List[DqField]] = None,
        primary_key_field_names: Optional[List[str]] = None,
        metadata: Optional[Dict[str, str]] = None,
        config: Optional[DqConfig] = None,
    ) -> DqResult:
        dq = Dq.fromDataFrame(self._sdf).rules(rules).fields(fields)
        dq.primaryKeyFieldNames(primary_key_field_names).metadata(metadata).config(config)
        return dq.dqResult()

    def dq_equi_join(
        self,
        right_sdf: SparkDataFrame,
        left_column_names: List[str],
        right_column_names: List[str],
        metadata: Optional[Dict[str, str]] = None,
        config: Optional[DqConfig] = None,
    ) -> DqJoinResult:
        dq_join = DqJoin.equiJoin(self._sdf, right_sdf, left_column_names, right_column_names)
        dq_join.metadata(metadata).config(config)
        return dq_join.dqJoinResult()


def init() -> None:
    SparkDataFrame.stoys = property(lambda sdf: PysparkDataFrameExtension(sdf))
