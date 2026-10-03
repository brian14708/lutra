"""Prepare uv-owned runtime selections at submission time."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import tempfile
import tomllib
from dataclasses import dataclass
from glob import has_magic
from pathlib import Path
from typing import TYPE_CHECKING, Any

import tomli_w

if TYPE_CHECKING:
    from collections.abc import Sequence

UV_VERSION = "0.12.11"
RECIPE = "lutra.uv-native.v1"
_SCRIPT_BLOCK = re.compile(r"(?ms)^# /// script[ \t]*\n.*?^# ///[ \t]*(?:\n|$)")
_LOCAL_KINDS = ("editable", "directory", "virtual")


def _metadata(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def _script_metadata(path: Path) -> dict[str, Any] | None:
    match = _SCRIPT_BLOCK.search(path.read_text(encoding="utf-8"))
    if match is None:
        return None
    return tomllib.loads(
        "\n".join(
            line.removeprefix("#").removeprefix(" ") for line in match.group().splitlines()[1:-1]
        )
    )


def _safe_path(path: Path) -> Path:
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        msg = f"source path must not follow a symlink: {path}"
        raise ValueError(msg)
    return path.resolve()


def _uv(root: Path, *args: str) -> bytes:
    executable = shutil.which("uv")
    if executable is None:
        msg = f"uv {UV_VERSION} is required to prepare a Python task environment"
        raise ValueError(msg)
    try:
        result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
            [executable, *args], cwd=root, capture_output=True, check=False
        )
    except FileNotFoundError as exc:
        msg = f"uv {UV_VERSION} is required to prepare a Python task environment"
        raise ValueError(msg) from exc
    if result.returncode:
        msg = f"uv {' '.join(args)} failed: {result.stderr.decode(errors='replace').strip()}"
        raise ValueError(msg)
    return result.stdout


def _owner(source_file: Path) -> tuple[Path, Path]:
    """Find the nearest project and its owning workspace, without invoking uv.

    Returns:
        The lock-owning root and task-owning project.

    Raises:
        ValueError: If the task has no owning project.

    """
    project = next((p for p in source_file.parents if (p / "pyproject.toml").is_file()), None)
    if project is None:
        msg = f"task source must belong to a uv project or PEP 723 script: {source_file}"
        raise ValueError(msg)
    for parent in project.parents:
        manifest = parent / "pyproject.toml"
        if not manifest.is_file():
            continue
        workspace = _metadata(manifest).get("tool", {}).get("uv", {}).get("workspace")
        if workspace is None:
            continue
        members = {
            p.resolve() for pattern in workspace.get("members", []) for p in parent.glob(pattern)
        }
        excluded = {
            p.resolve() for pattern in workspace.get("exclude", []) for p in parent.glob(pattern)
        }
        if project in members - excluded:
            return parent, project
        break
    return project, project


def _includes(root: Path, metadata: dict[str, Any]) -> tuple[Path, ...]:
    entries = metadata.get("tool", {}).get("lutra", {}).get("source-includes", [])
    if not isinstance(entries, list) or any(
        not isinstance(entry, str) or not entry or Path(entry).is_absolute() for entry in entries
    ):
        msg = "tool.lutra.source-includes must be a list of relative paths"
        raise ValueError(msg)
    return tuple(root / entry for entry in entries)


def _local_sources(locked: dict[str, Any]) -> list[tuple[str, str]]:
    """Find packages declared with local sources in a uv lock.

    Returns:
        Package name and lock-declared location pairs.

    """
    return [
        (p["name"], p["source"][kind])
        for p in locked.get("package", [])
        for kind in _LOCAL_KINDS
        if kind in p.get("source", {})
    ]


@dataclass(frozen=True)
class UvSource:
    """One prepared lock owner and uv's selected local source directories."""

    root: Path
    bundle_root: Path
    lock: Path
    project_roots: tuple[Path, ...]
    task_roots: tuple[Path, ...]
    includes: tuple[Path, ...]
    python_requires: str
    build_files: dict[str, bytes]
    runtime_files: dict[str, bytes]
    script: Path | None = None
    excluded_roots: tuple[Path, ...] = ()
    dependency_groups: tuple[str, ...] | None = None
    extras: tuple[str, ...] | None = None

    @property
    def prepare_command(self) -> tuple[str, ...]:
        if not self.runtime_files:
            return ("true",)
        args = [
            "uv",
            "sync",
            "--frozen",
            "--active",
            "--no-config",
            "--script",
            "/workspace/lutra-runtime.py",
        ]
        if self.dependency_groups is not None:
            args.append("--no-default-groups")
            args.extend(arg for group in self.dependency_groups for arg in ("--group", group))
        if self.extras:
            args.extend(arg for extra in self.extras for arg in ("--extra", extra))
        return ("sh", "-c", shlex.join(args))

    @property
    def required_files(self) -> tuple[Path, ...]:
        files = [self.lock]
        if self.script is not None:
            files.append(self.script)
        else:
            files.append(self.root / "pyproject.toml")
        files.extend(local / "pyproject.toml" for local in self.project_roots)
        return tuple(path.relative_to(self.bundle_root) for path in files)

    @property
    def python_paths(self) -> tuple[str, ...]:
        """Only non-installable task roots need Python search paths."""
        roots = set()
        if self.script is not None:
            roots.add(self.script.parent)
        for root in self.task_roots:
            metadata = _metadata(root / "pyproject.toml")
            uv = metadata.get("tool", {}).get("uv", {})
            if not uv.get("package", "build-system" in metadata):
                roots.add(root)
        return tuple(sorted(path.relative_to(self.bundle_root).as_posix() for path in roots))


def _prepare_lock(root: Path, lock: Path, script: Path | None) -> tuple[bytes, str]:
    script_args = ("--script", str(script)) if script else ()
    version = _uv(root, "--version").decode().split()[1]
    if version != UV_VERSION:
        msg = f"environment preparation requires uv {UV_VERSION}; found {version}"
        raise ValueError(msg)
    if not lock.exists():
        _uv(root, "lock", *script_args)
    else:
        try:
            _uv(root, "lock", "--check", "--offline", *script_args)
        except ValueError as exc:
            command = f"uv lock --script {script}" if script else f"uv lock --project {root}"
            msg = f"lock is stale or cannot be checked; run `{command}`: {exc}"
            raise ValueError(msg) from exc
    original_lock = lock.read_bytes()
    requirement = tomllib.loads(original_lock.decode()).get("requires-python")
    if not isinstance(requirement, str) or not requirement:
        msg = "uv lock must declare requires-python"
        raise ValueError(msg)
    return original_lock, requirement


def _native_lock(locked: dict[str, Any], paths: dict[str, str]) -> bytes:
    # Keep uv's resolution and markers intact. Only local locations change;
    # installable source directories use uv's native editable representation.
    normalized = {os.path.normpath(key): value for key, value in paths.items()}

    def rebase(value: Any) -> Any:  # ruff: ignore[any-type]
        if isinstance(value, list):
            return [rebase(item) for item in value]
        if not isinstance(value, dict):
            return value
        result: dict[str, Any] = {}
        for key, item in value.items():
            location = (
                os.path.normpath(item) if key in _LOCAL_KINDS and isinstance(item, str) else None
            )
            if location is not None and location in normalized:
                result["editable" if key == "directory" else key] = normalized[location]
            else:
                result[key] = rebase(item)
        return result

    return tomli_w.dumps(rebase(locked)).encode()


def _script_stub(requirement: str) -> bytes:
    return (
        "# /// script\n# requires-python = "
        + json.dumps(requirement)
        + "\n# dependencies = []\n# ///\n"
    ).encode()


def _default_groups(root: Path, locked: dict[str, Any]) -> tuple[str, ...]:
    metadata = _metadata(root / "pyproject.toml")
    configured = metadata.get("tool", {}).get("uv", {}).get("default-groups")
    if isinstance(configured, list) and all(isinstance(group, str) for group in configured):
        return tuple(configured)
    return ("dev",) if "dev" in locked.get("manifest", {}).get("dependency-groups", {}) else ()


def _selected_requirements(
    locked: dict[str, Any],
    names: list[str],
    groups: tuple[str, ...],
    extras: tuple[str, ...] | None,
) -> list[dict[str, Any]]:
    manifest = locked.get("manifest", {})
    available = manifest.get("dependency-groups", {})
    requirements: list[dict[str, Any]] = [{"name": name} for name in names]
    if extras:
        requirements = [{"name": name, "extras": list(extras)} for name in names]
    for group in groups:
        entries = available.get(group)
        if entries is None:
            entries = next(
                (
                    package.get("dev-dependencies", {}).get(group)
                    for package in locked.get("package", [])
                    if package.get("name") in names
                ),
                None,
            )
        if entries is None:
            msg = f"dependency group {group!r} is not defined in uv.lock"
            raise ValueError(msg)
        requirements.extend(entries)
    return requirements


def _selected_projects(root: Path, locked: dict[str, Any], stub: bytes) -> tuple[set[Path], bool]:
    paths = {location: str(_safe_path(root / location)) for _, location in _local_sources(locked)}
    with tempfile.TemporaryDirectory() as temporary:
        script = Path(temporary) / "environment.py"
        script.write_bytes(stub)
        script.with_suffix(".py.lock").write_bytes(_native_lock(locked, paths))
        graph = json.loads(
            _uv(
                root, "tree", "--frozen", "--universal", "--format", "json", "--script", str(script)
            )
        )
    projects = set()
    installable = False
    for package in graph["resolution"].values():
        source = package.get("source", {})
        for kind in _LOCAL_KINDS:
            if kind in source:
                projects.add(_safe_path(Path(source[kind])))
                installable |= kind != "virtual"
    return projects, installable


def _sync_inputs(  # ruff: ignore[too-many-arguments]
    root: Path,
    bundle_root: Path,
    locked: dict[str, Any],
    stub: bytes,
    *,
    installable: bool,
    dependency_groups: tuple[str, ...] | None,
    extras: tuple[str, ...] | None,
    selected_groups: tuple[str, ...],
) -> tuple[dict[str, bytes], dict[str, bytes]]:
    locations = {location: name for name, location in _local_sources(locked)}
    # These stable image paths have no relationship to the submitter's directory.
    image_paths = {location: "local/" + name for location, name in locations.items()}
    selectors: list[str] = []
    if dependency_groups is not None:
        selectors.append("--no-default-groups")
    selectors.extend(arg for group in selected_groups for arg in ("--group", group))
    if extras:
        selectors.extend(arg for extra in extras for arg in ("--extra", extra))
    build = {
        "environment.py": stub,
        "environment.py.lock": _native_lock(locked, image_paths),
        "sync.sh": (
            shlex.join([
                "uv",
                "sync",
                "--frozen",
                "--active",
                "--no-config",
                "--script",
                "/opt/lutra/dependencies/environment.py",
                *selectors,
                *(
                    arg
                    for name in sorted(set(locations.values()))
                    for arg in ("--no-install-package", name)
                ),
            ])
            + "\n"
        ).encode(),
    }
    runtime: dict[str, bytes] = {}
    if installable:
        runtime_paths = {
            location: os.path.relpath(_safe_path(root / location), bundle_root)
            for location in locations
        }
        runtime = {
            "lutra-runtime.py": stub,
            "lutra-runtime.py.lock": _native_lock(locked, runtime_paths),
        }
    return build, runtime


def _bundle_layout(
    root: Path, projects: set[Path], script: Path | None
) -> tuple[Path, tuple[Path, ...]]:
    includes = list(_includes(script.parent, _script_metadata(script) or {})) if script else []
    for project in sorted(projects | ({root} if not script else set())):
        if not (project / "pyproject.toml").is_file():
            msg = f"local uv dependency must be a project directory: {project}"
            raise ValueError(msg)
        includes.extend(_includes(project, _metadata(project / "pyproject.toml")))
    # The common ancestor determines archive paths, never the scan scope.
    anchors = [root, *projects]
    for include in includes:
        parts = include.parts
        first_magic = next((i for i, part in enumerate(parts) if has_magic(part)), len(parts))
        anchor = _safe_path(Path(*parts[:first_magic]))
        anchors.append(anchor if anchor.is_dir() else anchor.parent)
    return Path(os.path.commonpath(anchors)), tuple(includes)


def prepare_source(  # ruff: ignore[too-many-locals]
    task_files: Sequence[Path],
    *,
    dependency_groups: tuple[str, ...] | None = None,
    extras: tuple[str, ...] | None = None,
) -> UvSource:
    """Lock, validate and export the single environment owning these task files.

    Returns:
        Prepared dependency inputs and the selected runtime source layout.

    Raises:
        ValueError: If locks, task owners or local sources are invalid.

    """
    if not task_files:
        msg = "an environment must declare at least one task"
        raise ValueError(msg)
    files = tuple(_safe_path(path) for path in task_files)
    scripts = [path for path in files if _script_metadata(path) is not None]
    if len(set(scripts)) > 1:
        msg = "all environment tasks must share one locked project or script"
        raise ValueError(msg)
    script = scripts[0] if scripts else None
    if script is not None and (dependency_groups is not None or extras):
        msg = "dependency groups and extras require a uv project"
        raise ValueError(msg)
    owners: list[tuple[Path, Path]] = [_owner(path) for path in files] if script is None else []
    if owners and len({owner for owner, _ in owners}) != 1:
        msg = "all environment tasks must share one locked project or script"
        raise ValueError(msg)
    root = script.parent if script else owners[0][0]
    lock = _safe_path(Path(f"{script}.lock") if script else root / "uv.lock")
    original_lock, requirement = _prepare_lock(root, lock, script)
    locked = tomllib.loads(original_lock.decode())
    task_roots = {project for _, project in owners}
    names = sorted(
        {_metadata(p / "pyproject.toml").get("project", {}).get("name", "") for p in task_roots}
        - {""}
    )
    # Native script sync has exactly these runtime roots, with no workspace or
    # default development groups. Frozen sync reads requirements from the lock.
    if script is None:
        groups = (
            dependency_groups if dependency_groups is not None else _default_groups(root, locked)
        )
        locked["manifest"] = {"requirements": _selected_requirements(locked, names, groups, extras)}
    else:
        groups = ()
    stub = _script_stub(requirement)
    projects, installable = _selected_projects(root, locked, stub)
    projects.update(task_roots)
    if script:
        for path in files:
            if path != script and not any(path.is_relative_to(p) for p in projects):
                msg = (
                    "all environment tasks must share one locked project or script "
                    "(or its declared local dependencies)"
                )
                raise ValueError(msg)
        task_roots = {
            max((p for p in projects if path.is_relative_to(p)), key=lambda p: len(p.parts))
            for path in files
            if path != script
        }
    bundle_root, includes = _bundle_layout(root, projects, script)
    build_files, runtime_files = _sync_inputs(
        root,
        bundle_root,
        locked,
        stub,
        installable=installable,
        dependency_groups=dependency_groups,
        extras=extras,
        selected_groups=groups,
    )
    build_files["uv.lock"] = original_lock
    build_files["selection.json"] = json.dumps(
        {
            "recipe": RECIPE,
            "uv": UV_VERSION,
            "kind": "script" if script else "project",
            "packages": names,
            "dependency_groups": dependency_groups,
            "extras": extras,
        },
        sort_keys=True,
    ).encode()
    # Omit unrelated workspace members even when an owning root is scanned.
    all_local = {_safe_path(root / location) for _, location in _local_sources(locked)}
    return UvSource(
        root,
        bundle_root,
        lock,
        tuple(sorted(projects)),
        tuple(sorted(task_roots)),
        tuple(includes),
        requirement,
        build_files,
        runtime_files,
        script,
        tuple(sorted(all_local - projects - {root})),
        dependency_groups,
        extras,
    )
