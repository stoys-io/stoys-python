from typing import Any, Type

import serde.json


class Json:
    @staticmethod
    def to_json(value: Any) -> str:
        return serde.json.to_json(value)

    @staticmethod
    def from_json(cls: Type, json: str) -> str:
        return serde.json.from_json(cls, json)
