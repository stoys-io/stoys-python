from py4j.java_gateway import JavaObject
from pyspark.sql import SparkSession


def test_spark_session_fixture(spark_session: SparkSession) -> None:
    sdf = spark_session.createDataFrame([{"k": "1", "v": 42.0, "e": "foo"}])
    assert sorted(sdf.columns) == ["e", "k", "v"]
    import stoys

    assert isinstance(stoys.scala.Jackson.json, JavaObject)
