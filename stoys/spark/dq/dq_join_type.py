from enum import auto

from ...utils.python import AutoNameEnum


class DqJoinType(AutoNameEnum):
    UNDEFINED = auto()
    LEFT = auto()
    RIGHT = auto()
    INNER = auto()
    FULL = auto()
    CROSS = auto()
