from typing import Any, Dict

from .utils import stoys_ui_component_html

_default_dq_join_result_ui_config = dict(
    # mode="row",
    # mode="column",
    smallSize=True,
    # selectedRules=[],
    pagination=False,
    # statisticsTableProps={},
    # qualityTableProps={},
)


def dq_join_result_to_html(*dq_join_result, **kwargs: Dict[str, Any]) -> str:  # noqa: ANN002
    payload = dict(
        data=list(dq_join_result),
        config={**_default_dq_join_result_ui_config, **kwargs},
    )
    return stoys_ui_component_html("JoinRates", payload)
