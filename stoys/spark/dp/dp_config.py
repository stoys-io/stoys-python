from __future__ import annotations

from typing import Optional

from ...utils.case_class import CaseClassMirror, case_class_mirror
from ...utils.python import classproperty
from .dp_type_inference_config import DpTypeInferenceConfig


@case_class_mirror("io.stoys.spark.dp.DpConfig")
class DpConfig(CaseClassMirror):
    max_item_length: int
    items: int
    pmf_buckets: int
    time_zone_id: Optional[str]
    infer_types_from_strings: bool
    type_inference_config: DpTypeInferenceConfig

    @classproperty
    def default(cls) -> DpConfig:  # noqa: B902, N805
        return cls.from_jvm(cls.java_class.default())
