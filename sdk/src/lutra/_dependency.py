"""Discover locked uv projects and scripts for task bundling."""

from __future__ import annotations

import os
import re
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import tomllib
from dataclasses import dataclass
from pathlib import Path

_SCRIPT_BLOCK = re.compile(r"(?ms)^# /// script[ \t]*\n.*?^# ///[ \t]*(?:\n|$)")
_LOCAL_KINDS = ("editable", "directory", "virtual", "path")


@dataclass(frozen=True)
class UvSource:
    """A uv project or locked PEP 723 script and its local packages."""

    root: Path
    bundle_root: Path
    lock: Path
    local_roots: tuple[Path, ...]
    project_roots: tuple[Path, ...]
    script: Path | None = None

    @property
    def identity(self) -> tuple[str, Path]:
        return ("uv", self.lock)

    @property
    def is_script(self) -> bool:
        return self.script is not None

    @property
    def import_roots(self) -> tuple[Path, ...]:
        roots = {local.relative_to(self.bundle_root) for local in self.local_roots}
        for local in self.local_roots:
            if (local / "src").is_dir():
                roots.add((local / "src").relative_to(self.bundle_root))
        return tuple(sorted(roots, key=Path.as_posix))

    @property
    def local_package_names(self) -> tuple[str, ...]:
        """Local packages whose sources are supplied by the source bundle."""
        lock = tomllib.loads(self.lock.read_text(encoding="utf-8"))
        return tuple(
            sorted({
                package["name"]
                for package in lock.get("package", [])
                if any(kind in package.get("source", {}) for kind in _LOCAL_KINDS)
            })
        )

    @property
    def required_files(self) -> tuple[Path, ...]:
        files = [self.lock]
        if self.script is not None:
            files.append(self.script)
        files.extend(local / "pyproject.toml" for local in self.project_roots)
        return tuple(path.relative_to(self.bundle_root) for path in files)

    @property
    def build_files(self) -> dict[str, bytes]:
        files = {self.lock.relative_to(self.bundle_root).as_posix(): self.lock.read_bytes()}
        if self.script is not None:
            match = _SCRIPT_BLOCK.search(self.script.read_text(encoding="utf-8"))
            if match is None:
                msg = "PEP 723 metadata block is missing"
                raise ValueError(msg)
            files[self.script.relative_to(self.bundle_root).as_posix()] = match.group().encode()
        for local in self.project_roots:
            manifest = local / "pyproject.toml"
            files[manifest.relative_to(self.bundle_root).as_posix()] = manifest.read_bytes()
        for parent in {*self.root.parents, self.root, *self.local_roots}:
            if not parent.is_relative_to(self.bundle_root):
                continue
            for name in ("uv.toml", ".uv.toml", ".python-version"):
                config = parent / name
                if config.is_file():
                    files[config.relative_to(self.bundle_root).as_posix()] = config.read_bytes()
        return files

    @property
    def script_includes(self) -> tuple[Path, ...]:
        """Explicit sources and assets relative to the PEP 723 script.

        Raises:
            ValueError: If the inline extension is malformed.

        """
        if self.script is None:
            return ()
        match = _SCRIPT_BLOCK.search(self.script.read_text(encoding="utf-8"))
        if match is None:
            msg = "PEP 723 metadata block is missing"
            raise ValueError(msg)
        metadata = tomllib.loads("\n".join(line[2:] for line in match.group().splitlines()[1:-1]))
        includes = metadata.get("tool", {}).get("lutra", {}).get("source-includes", [])
        if not isinstance(includes, list) or any(
            not isinstance(entry, str) or not entry or Path(entry).is_absolute()
            for entry in includes
        ):
            msg = "PEP 723 tool.lutra.source-includes must be a list of relative paths"
            raise ValueError(msg)
        return tuple(self.script.parent / entry for entry in includes)

    def check_lock(self) -> str:
        args = ["uv", "lock", "--check", "--offline"]
        if self.script is not None:
            args.extend(("--script", str(self.script)))
        try:
            result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
                args, cwd=self.root, capture_output=True, check=False
            )
        except FileNotFoundError as exc:
            msg = "uv is required to prepare a Python task environment"
            raise ValueError(msg) from exc
        if result.returncode:
            msg = f"uv lock check failed: {result.stderr.decode().strip()}"
            raise ValueError(msg)
        requirement = tomllib.loads(self.lock.read_text(encoding="utf-8")).get("requires-python")
        if not isinstance(requirement, str) or not requirement:
            msg = "uv lock must declare requires-python"
            raise ValueError(msg)
        return requirement


def _repository_root(path: Path) -> Path | None:
    for parent in (path, *path.parents):
        if (parent / ".jj").exists() or (parent / ".git").exists():
            return parent
    runtime_root = os.environ.get("LUTRA_BUNDLE_ROOT")
    if runtime_root is not None:
        boundary = Path(runtime_root).resolve()
        if path.is_relative_to(boundary):
            return boundary
    return None


def _local_project(root: Path, location: str, repository: Path | None) -> Path:
    candidate = root / location
    if any(path.is_symlink() for path in (candidate, *candidate.parents)):
        msg = f"local uv dependency must not follow a symlink: {location}"
        raise ValueError(msg)
    candidate = candidate.resolve()
    if not candidate.is_dir():
        msg = f"local uv dependency must be a directory: {location}"
        raise ValueError(msg)
    if _repository_root(candidate) != repository or (
        not candidate.is_relative_to(root)
        and (repository is None or not candidate.is_relative_to(repository))
    ):
        msg = f"local uv dependency leaves repository: {location}"
        raise ValueError(msg)
    return candidate


def _script_lock(script: Path) -> Path:
    lock = Path(f"{script}.lock")
    if lock.is_symlink():
        msg = f"PEP 723 sidecar lock must not be a symlink: {lock}"
        raise ValueError(msg)
    if lock.is_file():
        return lock
    args = ["uv", "lock", "--script", str(script)]
    try:
        result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
            args, cwd=script.parent, capture_output=True, check=False
        )
    except FileNotFoundError as exc:
        msg = "uv is required to create a PEP 723 sidecar lock"
        raise ValueError(msg) from exc
    if result.returncode:
        detail = result.stderr.decode(errors="replace").strip()
        msg = f"uv script lock failed for {script}: {detail}"
        raise ValueError(msg)
    if not lock.is_file():
        msg = f"uv did not create the PEP 723 sidecar lock: {lock}"
        raise ValueError(msg)
    return lock


def discover_source(source_file: Path) -> UvSource:
    """Find the locked uv source that owns a task file, creating missing script locks.

    Returns:
        The locked source and its local package directories.

    Raises:
        ValueError: If a project lock is missing, script locking fails, or a local
            package crosses a repository boundary.

    """
    source_file = source_file.resolve()
    if _SCRIPT_BLOCK.search(source_file.read_text(encoding="utf-8")):
        root = source_file.parent
        lock = _script_lock(source_file)
        script = source_file
    else:
        for path in source_file.parents:
            if (path / "pyproject.toml").is_file() and (path / "uv.lock").is_file():
                root, lock, script = path, path / "uv.lock", None
                break
        else:
            msg = "task source must have a uv lock or PEP 723 sidecar lock"
            raise ValueError(msg)
    data = tomllib.loads(lock.read_text(encoding="utf-8"))
    local = {root}
    projects = {root} if script is None else set()
    repository = _repository_root(root)
    for package in data.get("package", []):
        package_source = package.get("source", {})
        for kind in _LOCAL_KINDS:
            if kind not in package_source:
                continue
            candidate = _local_project(root, package_source[kind], repository)
            local.add(candidate)
            projects.add(candidate)
    bundle_root = repository if repository is not None else root
    return UvSource(root, bundle_root, lock, tuple(sorted(local)), tuple(sorted(projects)), script)
