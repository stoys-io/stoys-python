from __future__ import annotations

from ...utils.case_class import CaseClassMirror, case_class_mirror
from ...utils.python import classproperty


@case_class_mirror("io.stoys.spark.dq.DqConfig")
class DqConfig(CaseClassMirror):
    sample_rows: bool
    max_rows_per_rule: int
    max_rows: int
    report_extra_columns: bool

    @classproperty
    def default(cls) -> DqConfig:  # noqa: B902, N805
        return cls.from_jvm(cls.java_class.default())
