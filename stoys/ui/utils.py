import uuid
from typing import Any

from ..utils.json import Json

DEFAULT_HEIGHT: str = "24rem"


def wrap_javascript_in_html(javascript: str) -> str:
    element_id = f"stoys-element-{uuid.uuid4()}"
    return f"""
        <span id="{element_id}"></span>
        <script>
            try {{
                let element = document.getElementById("{element_id}");
                {javascript}
            }} catch (error) {{
                let element = document.getElementById("{element_id}");
                element.innerHTML = "JavascriptError: " + error;
            }}
        </script>
    """


def stoys_ui_component_html(component_name: str, payload: Any) -> str:
    return wrap_javascript_in_html(
        f"""
        (function() {{
            const {{{component_name}, ReactDOM, React}} = stoysUi;
            ReactDOM.render(React.createElement({component_name}, {Json.to_json(payload)}, null), element);
        }})()
        """  # noqa: C812
    )
