import importlib
from enum import Enum
from typing import Any


def is_module_installed(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


class classproperty(property):  # noqa: N801
    def __get__(self, cls, owner) -> Any:  # noqa: ANN001
        return classmethod(self.fget).__get__(None, owner)()


class AutoNameEnum(Enum):
    def _generate_next_value_(name, start, count, last_values) -> str:  # noqa: ANN001, B902,N805
        return name
