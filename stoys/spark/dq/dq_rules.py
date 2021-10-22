from __future__ import annotations

from typing import List, Optional

from pyspark.sql.types import DataType

from ...utils.data_types import DataTypes
from ...utils.jvm import JvmPackage
from ...utils.py4j import Py4j
from .dq_field import DqField
from .dq_rule import DqRule

_jvm_dq = JvmPackage("io.stoys.spark.dq")


class DqRules:
    @classmethod
    def name(cls: DqRules, field_name: str, logical_name: str) -> str:
        # return f"{field_name}{LOGICAL_NAME_SEPARATOR}{logical_name}"
        return _jvm_dq.DqRules.name(field_name, logical_name)

    @classmethod
    def namedRule(
        cls: DqRules,
        field_name: str,
        logical_name: str,
        expression: str,
        description: Optional[str] = None,
    ) -> DqRule:
        # return DqRules.rule(DqRules.name(field_name, logical_name), expression, description)
        return DqRule.from_jvm(_jvm_dq.DqRules.namedRule(field_name, logical_name, expression, description))

    @classmethod
    def nullSafeNamedRule(
        cls: DqRules,
        field_name: str,
        logical_name: str,
        expression: str,
        description: Optional[str] = None,
    ) -> DqRule:
        # return DqRules.named_rule(
        #     field_name, logical_name, f"{SqlUtils.quoteIfNeeded(field_name)} IS NULL OR ({expression})", description
        # )
        return DqRule.from_jvm(_jvm_dq.DqRules.nullSafeNamedRule(field_name, logical_name, expression, description))

    # TODO: match all rule(...) variants from scala class
    @classmethod
    def rule(
        cls: DqRules,
        name: str,
        expression: str,
        description: Optional[str] = None,
    ) -> DqRule:
        # return DqRule(name, expression, description, [])
        return DqRule.from_jvm(_jvm_dq.DqRules.rule(name, expression, description))

    @classmethod
    def field(
        cls: DqRules,
        name: str,
        data_type_json: str,
        nullable: bool = True,
        enum_values: Optional[List[str]] = None,
        format: Optional[str] = None,
        regexp: Optional[str] = None,
    ) -> DqRule:
        jvm_enum_values = Py4j.toSeq(enum_values or [])
        return DqField.from_jvm(_jvm_dq.DqRules.field(name, data_type_json, nullable, jvm_enum_values, format, regexp))

    # common rules

    @classmethod
    def enumValuesRule(
        cls: DqRules,
        field_name: str,
        enum_values: Optional[List[str]] = None,
        case_insensitive: bool = True,
    ) -> DqRule:
        jvm_enum_values = Py4j.toSeq(enum_values or [])
        return DqRule.from_jvm(_jvm_dq.DqRules.enumValuesRule(field_name, jvm_enum_values, case_insensitive))

    @classmethod
    def notNullRule(
        cls: DqRules,
        field_name: str,
    ) -> DqRule:
        return DqRule.from_jvm(_jvm_dq.DqRules.notNullRule(field_name))

    @classmethod
    def regexpRule(
        cls: DqRules,
        field_name: str,
        regexp: Optional[str] = None,
    ) -> DqRule:
        return DqRule.from_jvm(_jvm_dq.DqRules.regexpRule(field_name, regexp))

    @classmethod
    def typeRule(
        cls: DqRules,
        field_name: str,
        source_type: DataType,
        target_type: DataType,
        format: Optional[str] = None,
    ) -> DqRule:
        jvm_source_type = DataTypes.to_jvm(source_type)
        jvm_target_type = DataTypes.to_jvm(target_type)
        return DqRule.from_jvm(_jvm_dq.DqRules.typeRule(field_name, jvm_source_type, jvm_target_type, format))

    # composite and multi field rules

    @classmethod
    def all(
        cls: DqRules,
        rule_name: str,
        rules: List[DqRule],
        description: Optional[str] = None,
    ) -> DqRule:
        return DqRule.from_jvm(_jvm_dq.DqRules.all(rule_name), Py4j.toSeq(r.to_jvm() for r in rules), description)

    @classmethod
    def any(
        cls: DqRules,
        rule_name: str,
        rules: List[DqRule],
        description: Optional[str] = None,
    ) -> DqRule:
        return DqRule.from_jvm(_jvm_dq.DqRules.any(rule_name), Py4j.toSeq(r.to_jvm() for r in rules), description)

    @classmethod
    def uniqueRule(
        cls: DqRules,
        base_rule_name: str,
        field_names: Optional[List[str]] = None,
    ) -> DqRule:
        if field_names is None:
            return DqRule.from_jvm(_jvm_dq.DqRules.uniqueRule(base_rule_name))
        else:
            return DqRule.from_jvm(_jvm_dq.DqRules.uniqueRule(base_rule_name, Py4j.toSeq(field_names)))
