from typing import Any, Dict

from .utils import DEFAULT_HEIGHT, stoys_ui_component_html

_default_dp_result_ui_config = dict(
    # colors=["red", "green", "blue",],
    height=DEFAULT_HEIGHT,
    smallSize=True,
    # orientType="Horizontal",
    # orientType="Vertical",
    pagination=False,
    # visibleColumns=["count_nulls", "count_unique", ],
    # showAxesSwitcher=True,
    # showChartTableSwitcher=True,
    # showJsonSwitcher=True,
    # showLogarithmicSwitcher=True,
    # showNormalizeSwitcher=True,
    # showOrientSwitcher=True,
    # showProfilerToolbar=True,
    # showSearch=True,
    # axesChecked=True,
    # chartTableChecked=True,
    # jsonChecked=True,
    # normalizeChecked=True,
    # logarithmicChecked=True,
)


def dp_result_to_html(*dp_result, **kwargs: Dict[str, Any]) -> str:  # noqa: ANN002
    payload = dict(
        datasets=list(dp_result),
        config={**_default_dp_result_ui_config, **kwargs},
    )
    return stoys_ui_component_html("Profiler", payload)
