"""Assemble backend-owned project sdists and explicitly included script files."""

from __future__ import annotations

import os
import shutil
import stat
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import tarfile
import tempfile
import tomllib
from dataclasses import dataclass, field
from glob import has_magic
from pathlib import Path
from typing import TYPE_CHECKING

from lutra._bundle import _MAX_FILE_SIZE, _MAX_FILES, _MAX_UNPACKED, _archive
from lutra._dependency import _repository_root
from lutra.archive import _safe_name

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from typing import IO

    from lutra._dependency import UvSource


@dataclass
class _Assembly:
    root: Path
    files: set[Path] = field(default_factory=set)
    size: int = 0

    def directory(self, relative: Path) -> None:
        if relative == Path():
            return
        _safe_name(relative.as_posix())
        if relative in self.files or any(parent in self.files for parent in relative.parents):
            msg = f"conflicting bundle paths: {relative}"
            raise ValueError(msg)
        (self.root / relative).mkdir(parents=True, exist_ok=True)

    def add(self, relative: Path, stream: IO[bytes], size: int, mode: int) -> None:
        _safe_name(relative.as_posix())
        target = self.root / relative
        duplicate = relative in self.files
        if (
            size < 0
            or size > _MAX_FILE_SIZE
            or self.size + (0 if duplicate else size) > _MAX_UNPACKED
        ):
            msg = "bundle exceeds extracted size limit"
            raise ValueError(msg)
        if not duplicate and len(self.files) >= _MAX_FILES:
            msg = "bundle has too many files"
            raise ValueError(msg)
        contents = stream.read(size + 1)
        if len(contents) != size:
            msg = f"bundle file size changed or is invalid: {relative}"
            raise ValueError(msg)
        executable = bool(mode & 0o111)
        if duplicate:
            if (
                target.read_bytes() == contents
                and bool(target.stat().st_mode & 0o111) == executable
            ):
                return
            msg = f"conflicting bundle contents: {relative}"
            raise ValueError(msg)
        if target.is_dir() or any(parent in self.files for parent in relative.parents):
            msg = f"conflicting bundle paths: {relative}"
            raise ValueError(msg)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(contents)
        target.chmod(0o755 if executable else 0o644)
        self.files.add(relative)
        self.size += size

    def local_file(self, root: Path, path: Path) -> None:
        relative = _local_path(root, path)
        info = path.stat()
        if not stat.S_ISREG(info.st_mode):
            msg = f"bundle file is not a regular file: {path}"
            raise ValueError(msg)
        with path.open("rb") as stream:
            self.add(relative, stream, info.st_size, info.st_mode)


def _local_path(root: Path, path: Path) -> Path:
    current = root
    if not path.is_relative_to(root) or not path.resolve().is_relative_to(root):
        msg = f"bundle path must be inside {root}: {path}"
        raise ValueError(msg)
    for part in path.relative_to(root).parts:
        current /= part
        if current.is_symlink():
            msg = f"included bundle path is a symlink: {path}"
            raise ValueError(msg)
    resolved = path.resolve()
    repository_path = resolved if path.is_dir() else resolved.parent
    if _repository_root(repository_path) != _repository_root(root):
        msg = f"included bundle path leaves repository: {path}"
        raise ValueError(msg)
    return path.resolve().relative_to(root)


def _include_files(root: Path, include: Path) -> Iterator[Path]:
    parts = include.parts
    first_magic = next((i for i, part in enumerate(parts) if has_magic(part)), len(parts))
    base = Path(*parts[:first_magic])
    _local_path(root, base)
    matches = base.glob("/".join(parts[first_magic:])) if first_magic < len(parts) else (base,)
    found = False
    for match in matches:
        _local_path(root, match)
        if match.is_dir():
            for directory, names, files in os.walk(match, followlinks=False):
                for name in sorted(names):
                    _local_path(root, Path(directory) / name)
                for name in sorted(files):
                    _local_path(root, Path(directory) / name)
                    found = True
                    yield Path(directory) / name
        elif match.exists():
            found = True
            yield match
    if not found:
        msg = f"included bundle path has no files: {include}"
        raise ValueError(msg)


def _unpack_sdist(assembly: _Assembly, archive: Path, prefix: Path) -> None:
    distribution: str | None = None
    size = 0
    try:  # ruff: ignore[too-many-statements-in-try-clause]
        with tarfile.open(archive, mode="r|gz") as tar:
            for count, member in enumerate(tar, start=1):
                parts = _safe_name(member.name)
                distribution = distribution or parts[0]
                if parts[0] != distribution or (len(parts) == 1 and not member.isdir()):
                    msg = "sdist must have a single distribution-directory prefix"
                    raise ValueError(msg)
                size += member.size
                if count > _MAX_FILES or size > _MAX_UNPACKED:
                    msg = "sdist exceeds size or entry limit"
                    raise ValueError(msg)
                if member.isdir():
                    if member.size:
                        msg = "sdist directory must not contain data"
                        raise ValueError(msg)
                    assembly.directory(prefix.joinpath(*parts[1:]))
                    continue
                if not member.isreg() or member.issparse():
                    msg = f"sdist entry is not a regular file: {member.name}"
                    raise ValueError(msg)
                stream = tar.extractfile(member)
                if stream is None:
                    msg = f"sdist file has no contents: {member.name}"
                    raise ValueError(msg)
                with stream:
                    assembly.add(prefix.joinpath(*parts[1:]), stream, member.size, member.mode)
    except tarfile.TarError as exc:
        msg = f"invalid sdist: {archive.name}"
        raise ValueError(msg) from exc
    if distribution is None:
        msg = "sdist is empty"
        raise ValueError(msg)


def _project_sdist(assembly: _Assembly, root: Path, project: Path) -> None:
    metadata = tomllib.loads((project / "pyproject.toml").read_text(encoding="utf-8"))
    if (
        "project" not in metadata
        and "build-system" not in metadata
        and "workspace" in metadata.get("tool", {}).get("uv", {})
    ):
        return
    uv = shutil.which("uv")
    if uv is None:
        msg = "uv is required to prepare a Python task environment"
        raise ValueError(msg)
    with tempfile.TemporaryDirectory() as temporary:
        result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
            [uv, "build", "--sdist", "--out-dir", temporary, str(project)],
            cwd=project,
            capture_output=True,
            check=False,
        )
        if result.returncode:
            detail = result.stderr.decode(errors="replace").strip()
            msg = f"uv sdist build failed for {project}: {detail}"
            raise ValueError(msg)
        archives = list(Path(temporary).glob("*.tar.gz"))
        if len(archives) != 1:
            msg = f"uv build must produce exactly one sdist for {project}"
            raise ValueError(msg)
        _unpack_sdist(assembly, archives[0], project.relative_to(root))


def build_source_bundle(source: UvSource, task_files: Sequence[Path]) -> bytes:
    """Build project sdists and assemble a deterministic source archive.

    Returns:
        Compressed source archive bytes.

    Raises:
        ValueError: If packaging fails, an input is unsafe, or an entrypoint is missing.

    """
    root = source.bundle_root
    with tempfile.TemporaryDirectory() as temporary:
        assembly = _Assembly(Path(temporary))
        for project in source.project_roots:
            _project_sdist(assembly, root, project)
        # Build inputs contain a metadata-only script; sources need the complete file.
        for name in source.build_files:
            assembly.local_file(root, root / name)
        for include in source.script_includes:
            for path in _include_files(root, include):
                assembly.local_file(root, path)
        for path in (*source.required_files, *task_files):
            if path not in assembly.files:
                msg = (
                    f"required task file is missing from the source bundle: {path}; "
                    "include it in the backend sdist packaging configuration"
                )
                raise ValueError(msg)
        bundle = _archive(assembly.root)
        if len(bundle) > 64 << 20:
            msg = "bundle exceeds 64 MiB"
            raise ValueError(msg)
        return bundle
