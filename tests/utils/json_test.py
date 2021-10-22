from dataclasses import dataclass
from typing import List

import serde

from stoys.utils.json import Json


@serde.deserialize
@serde.serialize
@dataclass
class Record:
    i: int
    s: str


def test_from_json():
    assert Json.from_json(Record, """{"i":42,"s":"foo"}""") == Record(42, "foo")
    assert Json.from_json(List[Record], """[{"i":42,"s":"foo"}]""") == [Record(42, "foo")]


def test_to_json():
    assert Json.to_json(Record(42, "foo")) == """{"i": 42, "s": "foo"}"""
    assert Json.to_json([Record(42, "foo")]) == """[{"i": 42, "s": "foo"}]"""
