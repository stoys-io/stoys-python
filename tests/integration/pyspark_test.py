import pyspark.sql.types as T  # noqa: N812
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812

import stoys


def test_pyspark_data_frame_extension(spark_session: SparkSession) -> None:
    sdf = spark_session.createDataFrame([{"k": "1", "v": 42.0, "e": "foo"}])

    target_struct = T.StructType([T.StructField("k", T.IntegerType())])
    assert sdf.stoys.reshape(target_struct).columns == ["k"]

    aggsum_sdf = sdf.groupBy("k").agg(F.sum(F.col("v")).alias("va"))
    assert aggsum_sdf.stoys.aggsum().aggsum_info.key_column_names == ["k"]

    assert sdf.stoys.dp().table.rows == 1

    rules = [stoys.spark.dq.DqRules.nullSafeNamedRule("k", "is_16_digits", "LENGTH(k) = 16")]
    assert sdf.stoys.dq(rules=rules).statistics.table.violations == 1

    lookup_sdf = spark_session.createDataFrame([{"id": "1", "description": "desc"}])
    assert sdf.stoys.dq_equi_join(lookup_sdf, ["k"], ["id"]).dq_join_statistics.inner == 1
