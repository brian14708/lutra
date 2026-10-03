"""Select project files and write deterministic source archives."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from glob import has_magic
from pathlib import Path
from typing import TYPE_CHECKING

from pathspec import GitIgnoreSpec

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

SOURCE_BUNDLE_MIME = "application/x-tar+zstd"
_MAX_FILES = 10_000
_MAX_FILE_SIZE = 128 << 20
_MAX_UNPACKED = 512 << 20

_EXCLUDED = {
    ".git",
    ".jj",
    ".lutra",
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
    ".cache",
    ".idea",
    ".egg-info",
    "node_modules",
    "dist",
    "build",
}


def _safe_relative(root: Path, path: Path) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve()
    if not resolved.is_relative_to(root) or resolved == root:
        msg = f"bundle path must be inside {root}: {path}"
        raise ValueError(msg)
    relative = resolved.relative_to(root)
    current = root
    for part in candidate.relative_to(root).parts:
        current /= part
        if current.is_symlink():
            msg = f"included bundle path is a symlink: {path}"
            raise ValueError(msg)
    if any(part in _EXCLUDED or part.startswith((".venv", ".env.")) for part in relative.parts):
        msg = f"bundle path is excluded: {path}"
        raise ValueError(msg)
    return relative


def _included_by_override(path: Path, includes: Sequence[Path]) -> bool:
    return any(path == include or path.is_relative_to(include) for include in includes)


def _expand_includes(root: Path, includes: Sequence[Path]) -> tuple[Path, ...]:
    overrides: list[Path] = []
    for include in includes:
        candidate = include if include.is_absolute() else root / include
        pattern = has_magic(str(candidate))
        parts = candidate.parts
        first_magic = next((i for i, part in enumerate(parts) if has_magic(part)), len(parts))
        matches = (
            Path(*parts[:first_magic]).glob("/".join(parts[first_magic:]))
            if pattern
            else (candidate,)
        )
        found = False
        for match in matches:
            if pattern and match.is_relative_to(root) and _excluded(match.relative_to(root)):
                continue
            overrides.append(_safe_relative(root, match))
            found = True
        if not found:
            msg = f"included bundle path has no files: {include}"
            raise ValueError(msg)
    return tuple(dict.fromkeys(overrides))


def _ignore_spec(directory: Path) -> GitIgnoreSpec | None:
    ignore = directory / ".gitignore"
    if ignore.is_file():
        return GitIgnoreSpec.from_lines(ignore.read_text(encoding="utf-8").splitlines())
    return None


def _ignored(path: Path, rules: Mapping[Path, GitIgnoreSpec], *, directory: bool = False) -> bool:
    ignored = False
    for parent in reversed(path.parents):
        if parent not in rules:
            continue
        name = path.relative_to(parent).as_posix() + ("/" if directory else "")
        result = rules[parent].check_file(name)
        if result.include is not None:
            ignored = result.include
    return ignored


def _excluded(path: Path) -> bool:
    return any(
        part in _EXCLUDED or part.startswith((".venv", ".env.")) for part in path.parts
    ) or path.suffix in {".pyc", ".pyo", ".egg", ".whl", ".log"}


def _selected_file(
    root: Path, relative: Path, rules: Mapping[Path, GitIgnoreSpec], overrides: Sequence[Path]
) -> bool:
    if _excluded(relative):
        return False
    if _ignored(relative, rules) and not _included_by_override(relative, overrides):
        return False
    source = root / relative
    if source.is_symlink() or not source.is_file():
        msg = f"bundle file is not a regular file: {relative}"
        raise ValueError(msg)
    return True


def _validate_limits(root: Path, selected: Sequence[Path]) -> None:
    if len(selected) > _MAX_FILES:
        msg = "bundle has too many files"
        raise ValueError(msg)
    sizes = [(root / path).stat().st_size for path in selected]
    if any(size > _MAX_FILE_SIZE for size in sizes) or sum(sizes) > _MAX_UNPACKED:
        msg = "bundle exceeds extracted size limit"
        raise ValueError(msg)


@dataclass
class _FileSelector:
    root: Path
    overrides: Sequence[Path]
    rules: dict[Path, GitIgnoreSpec] = field(default_factory=dict)
    blocked: set[Path] = field(default_factory=set)

    def scan(self, current: Path, names: list[str], files: list[str]) -> list[Path]:
        """Select files in one directory while tracking parent ignore rules.

        Returns:
            Selected paths relative to the source root.

        Raises:
            ValueError: If a selected directory is a symlink.

        """
        relative_dir = current.relative_to(self.root)
        is_blocked = any(
            relative_dir == path or relative_dir.is_relative_to(path) for path in self.blocked
        )
        spec = _ignore_spec(current) if not is_blocked else None
        if spec is not None:
            self.rules[relative_dir] = spec
        names[:] = sorted(
            name
            for name in names
            if name not in _EXCLUDED and not name.startswith((".venv", ".env."))
        )
        for name in names:
            relative = relative_dir / name
            if is_blocked or _ignored(relative, self.rules, directory=True):
                self.blocked.add(relative)
            if (current / name).is_symlink() and relative not in self.blocked:
                msg = f"bundle directory is a symlink: {relative}"
                raise ValueError(msg)
        selected = []
        for name in sorted(files):
            relative = relative_dir / name
            if is_blocked and not _included_by_override(relative, self.overrides):
                continue
            if _selected_file(self.root, relative, self.rules, self.overrides):
                selected.append(relative)
        return selected


def _scan_scope(selector: _FileSelector, scope: Path) -> list[Path]:
    root = selector.root
    if not scope.is_relative_to(root) or not scope.is_dir():
        msg = f"bundle scope must be inside {root}: {scope}"
        raise ValueError(msg)
    for parent in reversed((scope, *scope.parents)):
        if parent == scope or not parent.is_relative_to(root):
            continue
        spec = _ignore_spec(parent)
        if spec is not None:
            selector.rules[parent.relative_to(root)] = spec
    parts = scope.relative_to(root).parts
    for depth in range(1, len(parts) + 1):
        relative = Path(*parts[:depth])
        if _ignored(relative, selector.rules, directory=True):
            selector.blocked.add(relative)
            break
    selected: list[Path] = []
    for directory, names, files in os.walk(scope, followlinks=False):
        selected.extend(selector.scan(Path(directory), names, files))
    return selected


def select_files(
    root: Path, *, includes: Sequence[Path] = (), scopes: Sequence[Path] | None = None
) -> tuple[Path, ...]:
    """Select regular files using nested .gitignore rules.

    Explicit includes override ignore files, but not safety exclusions.

    Returns:
        Sorted paths relative to root.

    Raises:
        ValueError: If an included path is unsafe or a selected file is a symlink.

    """
    root = root.resolve()
    if not root.is_dir():
        msg = "bundle context must be a directory"
        raise ValueError(msg)
    overrides = _expand_includes(root, includes)
    selector = _FileSelector(root, overrides)
    selected: list[Path] = []
    directories = (
        (root,)
        if scopes is None
        else tuple(
            sorted(
                {path.resolve() for path in scopes},
                key=lambda path: (len(path.parts), path.as_posix()),
            )
        )
    )
    for scope in directories:
        if any(scope != parent and scope.is_relative_to(parent) for parent in directories):
            continue
        selected.extend(_scan_scope(selector, scope))
    for override in overrides:
        path = root / override
        if any(path.is_relative_to(scope) for scope in directories):
            continue
        if path.is_dir():
            selected.extend(_scan_scope(selector, path))
        elif path.is_file() and _selected_file(root, override, selector.rules, overrides):
            selected.append(override)
    selected = sorted(set(selected), key=Path.as_posix)
    missing = [
        path
        for path in overrides
        if not any(file == path or file.is_relative_to(path) for file in selected)
    ]
    if missing:
        msg = f"included bundle path has no files: {missing[0]}"
        raise ValueError(msg)
    _validate_limits(root, selected)
    return tuple(selected)


def resolve_context(root: Path, path: Path) -> Path:
    """Resolve a build context inside the locked source root.

    Returns:
        The resolved context directory.

    Raises:
        ValueError: If the path is absolute or leaves the locked root.

    """
    root = root.resolve()
    if path.is_absolute() or ".." in path.parts:
        msg = "image build context must be relative and inside the locked source root"
        raise ValueError(msg)
    current = root
    for part in path.parts:
        current /= part
        if current.is_symlink():
            msg = "image build context must not follow a symlink"
            raise ValueError(msg)
    context = (root / path).resolve()
    if not context.is_relative_to(root) or not context.is_dir():
        msg = "image build context must be a directory inside the locked source root"
        raise ValueError(msg)
    return context


def _archive(root: Path) -> bytes:
    from lutra.archive import create_archive  # ruff: ignore[import-outside-top-level]

    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "bundle.tar.zst"
        create_archive(root, path)
        return path.read_bytes()


def build_bundle(
    root: Path,
    *,
    includes: Sequence[Path] = (),
    selected: Sequence[Path] | None = None,
    generated: Mapping[str, bytes] | None = None,
) -> bytes:
    """Archive selected files without depending on a package manager.

    Returns:
        Deterministic compressed archive bytes.

    Raises:
        ValueError: If a generated file conflicts with a selected file.

    """
    root = root.resolve()
    files = selected if selected is not None else select_files(root, includes=includes)
    with tempfile.TemporaryDirectory() as temporary:
        staged = Path(temporary) / "bundle"
        staged.mkdir()
        for relative in files:
            source = root / relative
            target = staged / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())
            target.chmod(source.stat().st_mode & 0o777)
        for name, contents in sorted((generated or {}).items()):
            relative = _generated_path(name)
            target = staged / relative
            if target.is_file() and target.read_bytes() == contents:
                continue
            if target.exists():
                msg = f"generated bundle path conflicts with build context: {relative}"
                raise ValueError(msg)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(contents)
        return _archive(staged)


def _generated_path(name: str) -> Path:
    relative = Path(name)
    if relative.is_absolute() or relative.as_posix() != name or ".." in relative.parts:
        msg = f"unsafe generated bundle path: {name}"
        raise ValueError(msg)
    return relative


def bundle_bytes(files: Mapping[str, bytes]) -> bytes:
    """Archive generated files at validated relative paths.

    Returns:
        Deterministic compressed archive bytes.

    """
    with tempfile.TemporaryDirectory() as temporary:
        staged = Path(temporary) / "bundle"
        staged.mkdir()
        for name, contents in sorted(files.items()):
            relative = _generated_path(name)
            target = staged / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(contents)
        return _archive(staged)
