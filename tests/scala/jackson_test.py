from stoys.scala.jackson import Jackson
from stoys.utils.jvm import Jvm

cc_name = "io.stoys.spark.dq.DqRule"


def test_jackson() -> None:
    cc = Jvm.java_class(cc_name)
    jvm_rule = Jackson.fromJson(cc, '{"name": "foo"}')
    assert jvm_rule.toString() == "DqRule(foo,null,None,null)"
    assert Jackson.toJson(jvm_rule) == '{\n  "name" : "foo"\n}'
