"""Deterministic source bundle construction."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from lutra.archive import create_archive

if TYPE_CHECKING:
    from lutra.task import Task

R = TypeVar("R")
SOURCE_BUNDLE_MIME = "application/vnd.lutra.source-bundle+zstd"

_EXCLUDED = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    ".tox",
    "node_modules",
    "dist",
    "build",
}


def project_root(source_file: Path) -> Path:
    path = source_file.parent
    for directory in (path, *path.parents):
        if (directory / "pyproject.toml").is_file() and (directory / "uv.lock").is_file():
            return directory
    message = "task source must be inside a project with a locked uv environment"
    raise ValueError(message)


def build_bundle(task: Task[..., R]) -> bytes:
    root = project_root(task.source_file)
    files = sorted(
        (
            path
            for path in root.rglob("*")
            if path.is_file()
            and not path.is_symlink()
            and not any(
                part in _EXCLUDED or part.startswith(".venv")
                for part in path.relative_to(root).parts
            )
            and (path.suffix == ".py" or path.name in {"pyproject.toml", "uv.lock"})
        ),
        key=lambda path: path.relative_to(root).as_posix(),
    )
    with tempfile.TemporaryDirectory() as temporary:
        staged = Path(temporary) / "bundle"
        staged.mkdir()
        for source in files:
            target = staged / source.relative_to(root)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())
        archive = Path(temporary) / "bundle.tar.zst"
        create_archive(staged, archive)
        return archive.read_bytes()
