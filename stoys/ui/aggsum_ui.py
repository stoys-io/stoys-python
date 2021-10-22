import json
from typing import Any, Dict

from .utils import DEFAULT_HEIGHT, stoys_ui_component_html

_default_aggsum_result_ui_config = dict(
    previousReleaseDataIsShown=False,
    # disabledColumns=[],
    pagination=False,
    height=DEFAULT_HEIGHT,
    smallSize=True,
)


def aggsum_result_to_html(aggsum_result, previous_aggsum_result=None, **kwargs: Dict[str, Any]) -> str:  # noqa: ANN001
    payload = dict(
        data=dict(
            current=_aggsum_result_to_dict(aggsum_result),
            previous=_aggsum_result_to_dict(previous_aggsum_result),
        ),
        config={**_default_aggsum_result_ui_config, **kwargs},
    )
    return stoys_ui_component_html("Metrics", payload)


def _aggsum_result_to_dict(aggsum_result) -> Any:  # noqa: ANN001
    if aggsum_result is None:
        return None
    else:
        rtn = aggsum_result.aggsum_info.referenced_table_names
        return dict(
            table_name=rtn[0] if rtn else None,
            key_columns=aggsum_result.aggsum_info.key_column_names,
            data=json.loads(aggsum_result.data_json),
        )
