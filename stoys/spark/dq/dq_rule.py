from typing import List, Optional

from ...utils.case_class import CaseClassMirror, case_class_mirror


@case_class_mirror("io.stoys.spark.dq.DqRule")
class DqRule(CaseClassMirror):
    name: str
    expression: str
    description: Optional[str]
    referenced_column_names: List[str]
