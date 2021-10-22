from __future__ import annotations

from typing import Dict, List, Optional

from ..utils.case_class import CaseClassMirror, case_class_mirror
from ..utils.python import classproperty
from .reshape import ReshapeConfig


@case_class_mirror("io.stoys.spark.SparkIOConfig")
class SparkIOConfig(CaseClassMirror):
    input_paths: List[str]
    input_reshape_config: ReshapeConfig
    register_input_tables: bool
    output_path: Optional[str]
    write_format: Optional[str]
    write_mode: Optional[str]
    write_options: Dict[str, str]

    @classproperty
    def default(cls) -> SparkIOConfig:  # noqa: B902, N805
        return cls.from_jvm(cls.java_class.default())

    @classproperty
    def notebook(cls) -> SparkIOConfig:  # noqa: B902, N805
        return cls.from_jvm(cls.java_class.notebook())
