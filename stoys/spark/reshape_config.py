from __future__ import annotations

from enum import auto
from typing import Optional

from ..utils.case_class import CaseClassMirror, case_class_mirror
from ..utils.python import AutoNameEnum, classproperty


class ReshapeConflictResolution(AutoNameEnum):
    UNDEFINED = auto()
    ERROR = auto()
    FIRST = auto()
    LAST = auto()


class ReshapeSortOrder(AutoNameEnum):
    UNDEFINED = auto()
    ALPHABETICAL = auto()
    SOURCE = auto()
    TARGET = auto()


class ReshapeFieldMatchingStrategy(AutoNameEnum):
    UNDEFINED = auto()
    NAME_DEFAULT = auto()
    NAME_EXACT = auto()
    NAME_NORMALIZED = auto()
    INDEX = auto()


@case_class_mirror("io.stoys.spark.ReshapeConfig")
class ReshapeConfig(CaseClassMirror):
    coerce_types: bool
    conflict_resolution: ReshapeConflictResolution
    drop_extra_columns: bool
    fail_on_extra_column: bool
    fail_on_ignoring_nullability: bool
    fill_default_values: bool
    fill_missing_nulls: bool
    field_matching_strategy: ReshapeFieldMatchingStrategy
    sort_order: ReshapeSortOrder
    date_format: Optional[str]
    timestamp_format: Optional[str]

    @classproperty
    def spark(cls) -> ReshapeConfig:  # noqa: B902,N805
        return cls.from_jvm(cls.java_class.spark())

    @classproperty
    def safe(cls) -> ReshapeConfig:  # noqa: B902,N805
        return cls.from_jvm(cls.java_class.safe())

    @classproperty
    def default(cls) -> ReshapeConfig:  # noqa: B902,N805
        return cls.from_jvm(cls.java_class.default())

    @classproperty
    def dangerous(cls) -> ReshapeConfig:  # noqa: B902,N805
        return cls.from_jvm(cls.java_class.dangerous())

    @classproperty
    def notebook(cls) -> ReshapeConfig:  # noqa: B902,N805
        return cls.from_jvm(cls.java_class.notebook())
