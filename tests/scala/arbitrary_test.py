from stoys.scala.arbitrary import Arbitrary

cc_name = "io.stoys.spark.dq.DqRule"


def test_default() -> None:
    assert Arbitrary.default(cc_name).toString() == "DqRule(null,null,null,null)"


def test_empty() -> None:
    assert Arbitrary.empty(cc_name).toString() == "DqRule(null,null,None,List())"


def test_hashed() -> None:
    assert Arbitrary.hashed(cc_name, 42).toString() == "DqRule(4858178048799371839,9187444185395877681,None,List())"


def test_indexed() -> None:
    assert Arbitrary.indexed(cc_name, 42).toString() == "DqRule(42,43,None,List())"


def test_proto() -> None:
    assert Arbitrary.proto(cc_name).toString() == "DqRule(,,None,List())"


def test_random() -> None:
    assert Arbitrary.random(cc_name, 42).toString() == "DqRule(-5025562857975149833,-5843495416241995736,None,List())"
