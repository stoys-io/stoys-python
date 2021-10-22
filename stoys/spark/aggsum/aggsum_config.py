from __future__ import annotations

from typing import Optional

from ...utils.case_class import CaseClassMirror, case_class_mirror
from ...utils.python import classproperty


@case_class_mirror("io.stoys.spark.aggsum.AggSumConfig")
class AggSumConfig(CaseClassMirror):
    limit: Optional[bool]

    @classproperty
    def default(cls) -> AggSumConfig:  # noqa: B902, N805
        return cls.from_jvm(cls.java_class.default())
