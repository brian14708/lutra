"""Cached task declaration and action wire fields."""

import pytest
from lutra.task import TaskEnvironment


def _cached(value: int) -> int:
    return value


@pytest.mark.parametrize("version", [None, "1.2.3"])
def test_cached_action_sends_version(version: str | None) -> None:
    environment = TaskEnvironment("cache-test")
    task = environment.task(_cached, cache=True, version=version)
    spec = task(1).action_spec(1)
    assert spec.cache is True
    assert spec.task_version == (version or "")


def test_uncached_action_omits_cache_identity() -> None:
    environment = TaskEnvironment("cache-test")
    task = environment.task(_cached)
    spec = task(1).action_spec(1)
    assert spec.cache is False
    assert not spec.task_version
