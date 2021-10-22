from typing import Dict, List, Optional

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import DataType

from ..spark.aggsum import AggSum, AggSumConfig, AggSumResult
from ..spark.dp import Dp, DpConfig, DpResult
from ..spark.dq import Dq, DqConfig, DqField, DqJoin, DqJoinResult, DqResult, DqRule
from ..spark.reshape import Reshape
from ..spark.reshape_config import ReshapeConfig
from ..utils.spark_session import get_or_create_spark_session


@pd.api.extensions.register_dataframe_accessor("stoys")
class PandasDataFrameExtension:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df
        self._sdf = None

    def _to_spark_data_frame(self, df: pd.DataFrame) -> SparkDataFrame:
        return get_or_create_spark_session().createDataFrame(df)

    def _get_sdf(self) -> SparkDataFrame:
        if self._sdf is None:
            self._sdf = self._to_spark_data_frame(self._df)
        return self._sdf

    def reshape(
        self,
        target_schema: DataType,
        config: Optional[ReshapeConfig] = None,
    ) -> pd.DataFrame:
        return Reshape.reshapeToDF(self._get_sdf(), target_schema, config).toPandas()

    def aggsum(
        self,
        key_column_names: Optional[List[str]] = None,
        referenced_table_names: Optional[List[str]] = None,
        config: Optional[AggSumConfig] = None,
    ) -> AggSumResult:
        aggsum = AggSum.fromDataFrame(self._get_sdf()).config(config)
        aggsum.keyColumnNames(key_column_names or list(self._df.index.names))
        aggsum.referencedTableNames(referenced_table_names)
        return aggsum.aggSumResult()

    def dp(
        self,
        config: Optional[DpConfig] = None,
    ) -> DpResult:
        dp = Dp.fromDataFrame(self._get_sdf()).config(config)
        return dp.dpResult()

    def dq(
        self,
        rules: Optional[List[DqRule]] = None,
        fields: Optional[List[DqField]] = None,
        primary_key_field_names: Optional[List[str]] = None,
        metadata: Optional[Dict[str, str]] = None,
        config: Optional[DqConfig] = None,
    ) -> DqResult:
        dq = Dq.fromDataFrame(self._get_sdf()).rules(rules).fields(fields)
        dq.primaryKeyFieldNames(primary_key_field_names).metadata(metadata).config(config)
        return dq.dqResult()

    def dq_equi_join(
        self,
        right_df: pd.DataFrame,
        left_column_names: List[str],
        right_column_names: List[str],
        metadata: Optional[Dict[str, str]] = None,
        config: Optional[DqConfig] = None,
    ) -> DqJoinResult:
        right_sdf = self._to_spark_data_frame(right_df)
        dq_join = DqJoin.equiJoin(self._get_sdf(), right_sdf, left_column_names, right_column_names)
        dq_join.metadata(metadata).config(config)
        return dq_join.dqJoinResult()


def init() -> None:
    pass
