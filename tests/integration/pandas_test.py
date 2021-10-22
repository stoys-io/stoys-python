import pandas as pd
import pyspark.sql.types as T  # noqa: N812
from pyspark.sql import SparkSession

import stoys


def test_pandas_data_frame_extension(spark_session: SparkSession) -> None:
    df = pd.DataFrame([{"k": "1", "v": 42.0, "e": "foo"}])

    target_struct = T.StructType([T.StructField("k", T.IntegerType())])
    assert df.stoys.reshape(target_struct).columns == ["k"]

    aggsum_df = df.groupby("k").agg({"v": "sum"})
    assert aggsum_df.stoys.aggsum().aggsum_info.key_column_names == ["k"]

    assert df.stoys.dp().table.rows == 1

    rules = [stoys.spark.dq.DqRules.nullSafeNamedRule("k", "is_16_digits", "LENGTH(k) = 16")]
    assert df.stoys.dq(rules=rules).statistics.table.violations == 1

    lookup_df = pd.DataFrame([{"id": "1", "description": "desc"}])
    assert df.stoys.dq_equi_join(lookup_df, ["k"], ["id"]).dq_join_statistics.inner == 1
