"""Cached task declaration and action wire fields."""

from pathlib import Path

import pytest
from lutra import task as task_module
from lutra.task import TaskEnvironment


def _cached(value: int) -> int:
    return value


@pytest.mark.parametrize("version", [None, "1.2.3"])
def test_cached_action_sends_version_and_dependency_digest(
    monkeypatch: pytest.MonkeyPatch, version: str | None
) -> None:
    environment = TaskEnvironment("cache-test")
    task = environment.task(_cached, cache=True, version=version)
    monkeypatch.setattr(task, "dependency_key", lambda: bytes([7]) * 32)
    spec = task(1).action_spec(1)
    assert spec.cache is True
    assert spec.task_version == (version or "")
    assert spec.dependency_digest == bytes([7]) * 32


def test_uncached_action_omits_cache_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    environment = TaskEnvironment("cache-test")
    task = environment.task(_cached)
    monkeypatch.setattr(task, "dependency_key", lambda: pytest.fail("uncached action read digest"))
    spec = task(1).action_spec(1)
    assert spec.cache is False
    assert not spec.task_version
    assert spec.dependency_digest == b""


def test_dependency_digest_changes_with_lockfile(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'example'\n")
    lock = tmp_path / "uv.lock"
    lock.write_text("version = 1\n")

    def project_root(_source: Path) -> Path:
        return tmp_path

    monkeypatch.setattr(task_module, "project_root", project_root)
    environment = TaskEnvironment("cache-test")
    task = environment.task(_cached, cache=True)
    first = task(1).action_spec(1).dependency_digest
    assert first == task(1).action_spec(1).dependency_digest
    lock.write_text("version = 2\n")
    assert task(1).action_spec(1).dependency_digest != first
