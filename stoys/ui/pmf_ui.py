from typing import Any, Dict

from .utils import stoys_ui_component_html

_default_pmf_ui_config = dict(
    dataType="float",
    showAxes=True,
    showLogScale=False,
    color="red",
)


def pmf_to_html(*pmf, **kwargs: Dict[str, Any]) -> str:  # noqa: ANN002
    payload = dict(
        data=[pmf.pmf for pmf in pmf],
        config={**_default_pmf_ui_config, **kwargs},
    )
    return stoys_ui_component_html("PmfPlot", payload)
