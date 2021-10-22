from __future__ import annotations

from typing import List

from ...utils.case_class import CaseClassMirror, case_class_mirror
from ...utils.python import classproperty


@case_class_mirror("io.stoys.spark.dp.DpTypeInferenceConfig.EnumValues")
class EnumValues(CaseClassMirror):
    name: str
    values: List[str]


@case_class_mirror("io.stoys.spark.dp.DpTypeInferenceConfig")
class DpTypeInferenceConfig(CaseClassMirror):
    prefer_not_nullable: bool
    prefer_boolean_for_01: bool
    prefer_float: bool
    prefer_integer: bool
    date_formats: List[str]
    timestamp_formats: List[str]
    enum_values: List[EnumValues]

    @classproperty
    def default(cls) -> DpTypeInferenceConfig:  # noqa: B902, N805
        return cls.from_jvm(cls.java_class.default())
