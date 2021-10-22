from typing import List, Optional

from ...utils.case_class import CaseClassMirror, case_class_mirror


@case_class_mirror("io.stoys.spark.dq.DqField")
class DqField(CaseClassMirror):
    name: str
    data_type_json: str
    nullable: bool
    enum_values: List[str]
    format: Optional[str]
    regexp: Optional[str]
