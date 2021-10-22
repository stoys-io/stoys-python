from typing import List

from ...ui.dq_join_result_ui import dq_join_result_to_html
from ...utils.case_class import CaseClassMirror, case_class_mirror
from .dq_result import DqResult


@case_class_mirror("io.stoys.spark.dq.DqJoinInfo")
class DqJoinInfo(CaseClassMirror):
    left_table_name: str
    right_table_name: str
    left_key_column_names: List[str]
    right_key_column_names: List[str]
    join_type: str
    join_condition: str


@case_class_mirror("io.stoys.spark.dq.DqJoinStatistics")
class DqJoinStatistics(CaseClassMirror):
    left_rows: int
    right_rows: int
    left_nulls: int
    right_nulls: int
    left_distinct: int
    right_distinct: int
    inner: int
    left: int
    right: int
    full: int
    cross: int


@case_class_mirror("io.stoys.spark.dq.DqJoinResult")
class DqJoinResult(CaseClassMirror):
    key: str
    dq_join_info: DqJoinInfo
    dq_join_statistics: DqJoinStatistics
    dq_result: DqResult

    def _repr_html_(self) -> str:
        return dq_join_result_to_html(self)
