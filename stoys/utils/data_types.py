from py4j.java_gateway import JavaObject, is_instance_of
from pyspark.sql.types import DataType, StructType

from .jvm import JvmPackage

_jvm_spark_sql_types = JvmPackage("org.apache.spark.sql.types")


class DataTypes:
    @staticmethod
    def to_jvm(data_type: DataType) -> JavaObject:
        return _jvm_spark_sql_types.DataType.fromJson(data_type.json())

    @staticmethod
    def from_jvm(jvm_data_type: JavaObject) -> DataType:
        if is_instance_of(jvm_data_type, "org.apache.spark.sql.types.StructType"):
            return StructType.fromJson(jvm_data_type.json())
        else:
            empty_metadata = _jvm_spark_sql_types.Metadata.empty()
            jvm_struct_field = _jvm_spark_sql_types.StructField("dummy", jvm_data_type, True, empty_metadata)
            jvm_struct_type = _jvm_spark_sql_types.StructType([jvm_struct_field])
            struct_type = StructType.fromJson(jvm_struct_type.json())
            return struct_type.fields[0].dataType
