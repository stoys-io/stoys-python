from typing import Optional

from ..utils.jvm import JvmPackage
from ..utils.py4j import Py4j

_jvm_spark = JvmPackage("io.stoys.spark")


class TableName:
    def __init__(self, entity_name: str, logical_name: Optional[str] = None) -> None:
        self._table_name = _jvm_spark.TableName(entity_name, Py4j.toOption(logical_name), None)  # TODO: tag

    def full_table_name(self) -> str:
        self._table_name.fullTableName()
