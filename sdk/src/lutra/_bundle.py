"""Deterministic source bundle construction."""

from __future__ import annotations

import tempfile
from pathlib import Path

SOURCE_BUNDLE_MIME = "application/x-tar+zstd"

_EXCLUDED = {
    ".git",
    ".data",
    ".direnv",
    ".env",
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


def build_bundle(root: Path, *, source_only: bool = True) -> bytes:
    from lutra.archive import create_archive  # ruff: ignore[import-outside-top-level]

    if not root.is_dir():
        message = "bundle context must be a directory"
        raise ValueError(message)
    files = sorted(
        (
            path
            for path in root.rglob("*")
            if path.is_file()
            and not path.is_symlink()
            and not any(
                part in _EXCLUDED or part.startswith((".venv", ".env."))
                for part in path.relative_to(root).parts
            )
            and (
                not source_only
                or path.suffix == ".py"
                or path.name in {"pyproject.toml", "uv.lock"}
            )
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
            target.chmod(source.stat().st_mode & 0o777)
        archive = Path(temporary) / "bundle.tar.zst"
        create_archive(staged, archive)
        return archive.read_bytes()
