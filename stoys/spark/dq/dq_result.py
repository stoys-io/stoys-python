from typing import Dict, List

from ...utils.case_class import CaseClassMirror, case_class_mirror
from .dq_rule import DqRule


@case_class_mirror("io.stoys.spark.dq.DqColumn")
class DqColumn(CaseClassMirror):
    name: str


@case_class_mirror("io.stoys.spark.dq.DqTableStatistic")
class DqTableStatistic(CaseClassMirror):
    rows: int
    violations: int


@case_class_mirror("io.stoys.spark.dq.DqRuleStatistics")
class DqRuleStatistics(CaseClassMirror):
    rule_name: str
    violations: int


@case_class_mirror("io.stoys.spark.dq.DqColumnStatistics")
class DqColumnStatistics(CaseClassMirror):
    column_name: str
    violations: int


@case_class_mirror("io.stoys.spark.dq.DqStatistics")
class DqStatistics(CaseClassMirror):
    table: DqTableStatistic
    column: List[DqColumnStatistics]
    rule: List[DqRuleStatistics]


@case_class_mirror("io.stoys.spark.dq.DqRowSample")
class DqRowSample(CaseClassMirror):
    row: List[str]
    violated_rule_names: List[str]


@case_class_mirror("io.stoys.spark.dq.DqResult")
class DqResult(CaseClassMirror):
    columns: List[DqColumn]
    rules: List[DqRule]
    statistics: DqStatistics
    row_sample: List[DqRowSample]
    metadata: Dict[str, str]

    def _repr_html_(self) -> str:
        from ...ui.dq_result_ui import dq_result_to_html

        return dq_result_to_html(self)
