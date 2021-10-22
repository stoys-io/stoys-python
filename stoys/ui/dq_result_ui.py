from typing import Any, Dict

from .utils import stoys_ui_component_html

_default_dq_result_ui_config = dict(
    # mode="row",
    # mode="column",
    smallSize=True,
    # selectedRules=[],
    pagination=False,
    # showReferencedColumnsOnly=False,
    # rulesTableProps={},
    # sampleTableProps={},
)


def dq_result_to_html(dq_result, **kwargs: Dict[str, Any]) -> str:  # noqa: ANN001
    payload = dict(
        data=dq_result,
        config={**_default_dq_result_ui_config, **kwargs},
    )
    return stoys_ui_component_html("Quality", payload)
