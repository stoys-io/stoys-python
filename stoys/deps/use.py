from typing import List, Optional

from pyspark.sql import SparkSession

from ..config import (
    STOYS_LIB_CLASSIFIER,
    STOYS_LIB_GROUP_ID,
    STOYS_LIB_MODULES,
    STOYS_LIB_SCALA_COMPAT_VERSION,
    STOYS_LIB_VERSION,
)
from .common import embedded_dependencies_dir


def get_stoys_jars_packages() -> List[str]:
    def get_module_coords(module: str) -> str:
        return (
            f"{STOYS_LIB_GROUP_ID}:{module}_{STOYS_LIB_SCALA_COMPAT_VERSION}:{STOYS_LIB_VERSION}:{STOYS_LIB_CLASSIFIER}"
        )

    return [get_module_coords(module) for module in STOYS_LIB_MODULES]


def get_stoys_jars() -> List[str]:
    # return [ str(p) for p in embedded_dependencies_dir.glob("*.jar") ]
    def get_module_file_name(module: str) -> str:
        return f"{module}_{STOYS_LIB_SCALA_COMPAT_VERSION}-{STOYS_LIB_VERSION}-{STOYS_LIB_CLASSIFIER}.jar"

    return [str(embedded_dependencies_dir.joinpath(get_module_file_name(module))) for module in STOYS_LIB_MODULES]


def get_stoys_ui_js_content() -> str:
    return embedded_dependencies_dir.joinpath("stoys-ui.js").read_text(encoding="UTF8")


def _append_value_list(separator: str, current_value_list: Optional[str], *values: str) -> str:
    current_values = current_value_list.split(separator) if current_value_list is not None else []
    all_values = current_values + [v for v in values if v is not None and v not in current_values]
    return separator.join(all_values)


def add_stoys_jars(builder: SparkSession.Builder) -> SparkSession.Builder:
    current_jars = builder._options.get("spark.jars")
    jars = _append_value_list(",", current_jars, *get_stoys_jars())
    builder.config("spark.jars", jars)
    return builder


def add_stoys_jars_packages(builder: SparkSession.Builder) -> SparkSession.Builder:
    current_packages = builder._options.get("spark.jars.packages")
    packages = _append_value_list(",", current_packages, *get_stoys_jars_packages())
    builder.config("spark.jars.packages", packages)
    return builder
