from stoys.utils.scala_compat import Map, Option, Seq, Some


def test_option() -> None:
    assert Option(None) is None
    assert Option("foo") == "foo"
    assert Some("foo") == "foo"


def test_seq() -> None:
    assert Seq.empty == []
    assert Seq() == []
    assert Seq(1, 2, 3, 4) == [1, 2, 3, 4]


def test_map() -> None:
    assert Map.empty == {}
    assert Map() == {}
    assert Map(foo="foo", bar="bar") == {"foo": "foo", "bar": "bar"}
    assert Map(("foo", "foo")) == {"foo": "foo"}
    assert Map(("foo", "foo"), ("bar", "bar")) == {"foo": "foo", "bar": "bar"}
    # assert Map('foo' -> 'foo', 'bar' -> 'bar') == {'foo': 'foo', 'bar': 'bar'}
