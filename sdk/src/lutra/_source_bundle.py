"""Assemble deterministic snapshots of selected sources and runtime metadata."""

from __future__ import annotations

import io
import stat
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from lutra._bundle import _MAX_FILE_SIZE, _MAX_FILES, _MAX_UNPACKED, _archive, select_files
from lutra.archive import _safe_name

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import IO


@dataclass(frozen=True)
class PreparedSource:
    """Shared source layout for snapshots and task registration."""

    root: Path
    bundle_root: Path
    project_roots: tuple[Path, ...]
    includes: tuple[Path, ...]
    required_files: tuple[Path, ...]
    excluded_roots: tuple[Path, ...]
    runtime_files: dict[str, bytes]
    script: Path | None = None


@dataclass
class _Assembly:
    root: Path
    files: set[Path] = field(default_factory=set)
    size: int = 0

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
    return path.resolve().relative_to(root)


def build_source_bundle(source: PreparedSource, task_files: Sequence[Path]) -> bytes:
    """Snapshot selected projects, explicit assets, manifests and task files.

    Returns:
        Compressed deterministic source archive bytes.

    Raises:
        ValueError: If selected sources are unsafe or exceed archive limits.

    """
    root = source.bundle_root
    required = (*source.required_files, *task_files)
    selected = select_files(
        root,
        scopes=source.project_roots,
        includes=(*source.includes, *required),
        excludes=source.excluded_roots,
    )
    with tempfile.TemporaryDirectory() as temporary:
        assembly = _Assembly(Path(temporary))
        for relative in selected:
            assembly.local_file(root, root / relative)
        for name, contents in source.runtime_files.items():
            assembly.add(Path(name), io.BytesIO(contents), len(contents), 0o644)
        bundle = _archive(assembly.root)
        if len(bundle) > 64 << 20:
            msg = "bundle exceeds 64 MiB"
            raise ValueError(msg)
        return bundle
