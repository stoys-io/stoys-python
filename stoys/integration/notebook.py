import builtins
from typing import Any, Callable, Optional

from ..utils.python import is_module_installed


def is_ipython() -> bool:
    return getattr(builtins, "__IPYTHON__", False) and is_module_installed("IPython.core.display")


def is_databricks() -> bool:
    return is_ipython() and hasattr(globals(), "dbutils") and hasattr(globals(), "displayHTML")


def is_zeppelin() -> bool:
    return "ZeppelinContext" in str(type(getattr(globals(), "z", None)))


def is_colab() -> bool:
    if not is_ipython():
        return False
    else:
        from IPython.core.getipython import get_ipython

        return is_module_installed("google.colab") and "google.colab" in str(get_ipython())


_display_html_function: Optional[Callable[[str], Any]] = None


def _display_html_ipython(html: str) -> None:
    from IPython.core import display

    display.display(display.HTML(html))


def _display_html_databricks(html: str) -> None:
    global displayHTML
    displayHTML(html)  # type: ignore


def _display_html_zeppelin(html: str) -> None:
    print(f"%html {html}")


def _display_html_unsupported(html: str) -> None:
    print(f"No notebook environment detected! HTML snipped of length {len(html)} is ignored.")


def _detect_display_html_function() -> Optional[Callable[[str], Any]]:
    if is_ipython():
        if is_databricks():
            return _display_html_databricks
        return _display_html_ipython
    else:
        if is_zeppelin():
            return _display_html_zeppelin
    return _display_html_unsupported


def display_html(html: str) -> Any:
    global _display_html_function
    if _display_html_function is None:
        _display_html_function = _detect_display_html_function()
    return _display_html_function(html)
