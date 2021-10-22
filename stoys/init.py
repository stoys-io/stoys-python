from typing import Any

# from typeguard.importhook import install_import_hook

# install_import_hook("stoys")


def init() -> Any:
    from .utils.python import is_module_installed

    if is_module_installed("pyspark"):
        from pyspark.sql import SparkSession

        if SparkSession.getActiveSession() is None:
            from .utils.spark_session import _patch_spark_session_builder

            _patch_spark_session_builder()
        else:
            from .utils.spark_session import check_spark_session_with_stoys_library

            check_spark_session_with_stoys_library()
        from .integration import pyspark as pyspark_integration

        pyspark_integration.init()
    else:
        raise Exception("Stoys cannot work without spark! Please install package pyspark!") from None

    if is_module_installed("pandas"):
        from .integration import pandas as pandas_integration

        pandas_integration.init()

    if is_module_installed("IPython"):
        from .integration import ipython as ipython_integration

        ipython_integration.init()

    # TODO: koalas
    # TODO: pyspark.pandas

    from .deps.use import get_stoys_ui_js_content
    from .integration.notebook import display_html
    from .ui.utils import wrap_javascript_in_html

    return display_html(wrap_javascript_in_html(get_stoys_ui_js_content()))
