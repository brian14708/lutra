from lutra.serializer import (
    Serializer,
    available_serializers,
    deserialize,
    register_serializer,
    serialize,
)


class _UpperSerializer:
    name = "upper"
    version = "1"

    def serialize(self, value: object) -> bytes:  # ruff: ignore[no-self-use]
        return str(value).upper().encode()

    def deserialize(self, data: bytes) -> object:  # ruff: ignore[no-self-use]
        return data.decode().lower()


def test_builtin_serializers_round_trip() -> None:
    value = {"count": 3, "items": ["a", "b"]}
    assert deserialize(serialize(value, "json"), "json") == value
    assert deserialize(serialize(value, "pickle"), "pickle") == value
    assert available_serializers() == ("json", "pickle")


def test_serializer_protocol_accepts_custom_implementations() -> None:
    serializer = _UpperSerializer()
    assert isinstance(serializer, Serializer)
    register_serializer(serializer)
    assert deserialize(serialize("hello", "upper"), "upper") == "hello"
