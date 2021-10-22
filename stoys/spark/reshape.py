from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import DataType

from ..utils.data_types import DataTypes
from ..utils.jvm import JvmPackage
from .reshape_config import ReshapeConfig

_jvm_spark = JvmPackage("io.stoys.spark")


class Reshape:
    @staticmethod
    def reshapeToDF(sdf: SparkDataFrame, target_schema: DataType, config: ReshapeConfig = None) -> SparkDataFrame:
        jvm_target_schema = DataTypes.to_jvm(target_schema)
        jvm_config = (config or ReshapeConfig.default).to_jvm()
        reshaped_jdf = _jvm_spark.Reshape.reshapeToDF(sdf._jdf, jvm_target_schema, jvm_config)
        return SparkDataFrame(reshaped_jdf, sdf.sql_ctx)
