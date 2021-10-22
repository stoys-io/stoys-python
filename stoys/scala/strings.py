from ..utils.jvm import JvmPackage

_jvm_scala = JvmPackage("io.stoys.scala")


class Strings:
    @staticmethod
    def to_snake_case(value: str) -> str:
        return _jvm_scala.Strings.toSnakeCase(value)

    @staticmethod
    def trim(value: str) -> str:
        return _jvm_scala.Strings.trim(value)

    @staticmethod
    def to_word_characters(value: str) -> str:
        return _jvm_scala.Strings.toWordCharacters(value)

    @staticmethod
    def to_word_characters_collapsing(value: str) -> str:
        return _jvm_scala.Strings.toWordCharactersCollapsing(value)
