from typing import Optional

from pyspark.sql import DataFrame as SparkDataFrame

from ..utils.jvm import JvmPackage

_jvm_spark = JvmPackage("io.stoys.spark")


class Datasets:
    @staticmethod
    def getAlias(sdf: SparkDataFrame) -> Optional[str]:
        return _jvm_spark.Datasets.getAlias(sdf._jdf)
