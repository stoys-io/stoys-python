from typing import Dict, List, Optional

from ...utils.case_class import CaseClassMirror, case_class_mirror


@case_class_mirror("io.stoys.spark.dp.DpPmfBucket")
class DpPmfBucket(CaseClassMirror):
    low: float
    high: float
    count: int


@case_class_mirror("io.stoys.spark.dp.DpItem")
class DpItem(CaseClassMirror):
    item: str
    count: int


@case_class_mirror("io.stoys.spark.dp.DpColumn")
class DpColumn(CaseClassMirror):
    name: str
    data_type: str
    data_type_json: str
    nullable: bool
    enum_values: List[str]
    format: Optional[str]
    count: int
    count_empty: Optional[int]
    count_nulls: Optional[int]
    count_unique: Optional[int]
    count_zeros: Optional[int]
    max_length: Optional[int]
    min: Optional[str]
    max: Optional[str]
    mean: Optional[float]
    pmf: List[DpPmfBucket]
    items: List[DpItem]
    extras: Dict[str, str]


@case_class_mirror("io.stoys.spark.dp.DpTable")
class DpTable(CaseClassMirror):
    rows: int


@case_class_mirror("io.stoys.spark.dp.DpResult")
class DpResult(CaseClassMirror):
    table: DpTable
    columns: List[DpColumn]

    def _repr_html_(self) -> str:
        from ...ui.dp_ui import dp_result_to_html

        return dp_result_to_html(self)
