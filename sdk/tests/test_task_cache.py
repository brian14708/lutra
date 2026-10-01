"""Cached task declaration and action wire fields."""

from lutra.task import TaskEnvironment

_DIGEST_LENGTH = 32


def _cached(value: int) -> int:
    return value


def test_source_derived_version_sends_dependency_digest() -> None:
    environment = TaskEnvironment("cache-test")
    task = environment.task(_cached, cache=True)
    spec = task(1).action_spec(1)
    assert spec.cache is True
    assert not spec.task_version
    assert len(spec.dependency_digest) == _DIGEST_LENGTH


def test_explicit_version_is_preserved() -> None:
    environment = TaskEnvironment("cache-test")
    task = environment.task(_cached, cache=True, version="1.2.3")
    spec = task(1).action_spec(1)
    assert spec.task_version == "1.2.3"
    assert len(spec.dependency_digest) == _DIGEST_LENGTH
