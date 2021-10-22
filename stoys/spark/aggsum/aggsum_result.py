from typing import List

from ...utils.case_class import CaseClassMirror, case_class_mirror


@case_class_mirror("io.stoys.spark.aggsum.AggSumInfo")
class AggSumInfo(CaseClassMirror):
    key_column_names: List[str]
    value_column_names: List[str]
    referenced_table_names: List[str]


@case_class_mirror("io.stoys.spark.aggsum.AggSumResult")
class AggSumResult(CaseClassMirror):
    aggsum_info: AggSumInfo
    data_json: str  # df.to_json(orient='records')

    def _repr_html_(self) -> str:
        from ...ui.aggsum_ui import aggsum_result_to_html

        return aggsum_result_to_html(self)
