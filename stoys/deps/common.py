import pathlib

from ..config import STOYS_EMBEDDED_DEPENDENCIES_PACKAGE

_repository_dir = pathlib.Path(__file__).parent.parent.parent.resolve()
embedded_dependencies_dir = _repository_dir.joinpath(*STOYS_EMBEDDED_DEPENDENCIES_PACKAGE.split("."))
