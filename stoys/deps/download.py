import pathlib
import urllib.request
from typing import List

from ..config import (
    STOYS_LIB_CLASSIFIER,
    STOYS_LIB_GROUP_ID,
    STOYS_LIB_MODULES,
    STOYS_LIB_SCALA_COMPAT_VERSION,
    STOYS_LIB_VERSION,
    STOYS_UI_VERSION,
)
from .common import embedded_dependencies_dir


def _download_file(url: str, path: pathlib.Path) -> pathlib.Path:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        if url.lower().startswith("https"):
            urllib.request.urlretrieve(url, path)  # noqa: S310
        else:
            raise ValueError from None
    return path


def download_stoys_lib_jar(target_dir: pathlib.Path, module: str) -> List[pathlib.Path]:
    base_url = "https://repo1.maven.org/maven2"
    group_dir = STOYS_LIB_GROUP_ID.replace(".", "/")
    artifact_dir = f"{module}_{STOYS_LIB_SCALA_COMPAT_VERSION}/{STOYS_LIB_VERSION}"
    file_name = f"{module}_{STOYS_LIB_SCALA_COMPAT_VERSION}-{STOYS_LIB_VERSION}-{STOYS_LIB_CLASSIFIER}.jar"
    url = f"{base_url}/{group_dir}/{artifact_dir}/{file_name}"
    return _download_file(url, target_dir / file_name)


def download_stoys_ui_js(target_dir: pathlib.Path) -> pathlib.Path:
    base_url = "https://github.com/stoys-io/stoys-ui/releases/download"
    file_name = "stoys-ui.js"
    url = f"{base_url}/{STOYS_UI_VERSION}/{file_name}"
    return _download_file(url, target_dir / file_name)


def download_dependencies(
    dependencies_dir: pathlib.Path = embedded_dependencies_dir,
) -> List[pathlib.Path]:
    lib_deps = [download_stoys_lib_jar(dependencies_dir, m) for m in STOYS_LIB_MODULES]
    ui_deps = [download_stoys_ui_js(dependencies_dir)]
    return lib_deps + ui_deps


if __name__ == "__main__":
    download_dependencies()
