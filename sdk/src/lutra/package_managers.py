"""Submission-time package manager adapters for Linux images."""

from __future__ import annotations

import re
import shlex
import tomllib
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, TypeVar

from lutra._dependency import (
    UV_IMAGE,
    _local_sources,
    _native_lock,
    _owner,
    _safe_path,
    _script_metadata,
    prepare_source,
)

if TYPE_CHECKING:
    from lutra._source_bundle import PreparedSource


@dataclass(frozen=True)
class OciCopy:
    """Copy a file or directory from an OCI image, preserving its metadata.

    Tags may move. Use a digest reference to require identical image contents.
    The binary and its libraries must be compatible with the base image.
    """

    image: str
    source: str
    destination: str


@dataclass(frozen=True)
class ManagerOutput:
    """Build inputs and runtime commands produced by one manager."""

    build_files: dict[str, bytes] = field(default_factory=dict)
    runtime_files: dict[str, bytes] = field(default_factory=dict)
    oci_copies: tuple[OciCopy, ...] = ()
    build_commands: tuple[tuple[str, ...], ...] = ()
    prepare_commands: tuple[tuple[str, ...], ...] = ()
    build_env: dict[str, str] = field(default_factory=dict)
    wrapper: tuple[str, ...] = ()
    python: str | None = None
    source: PreparedSource | None = None
    entrypoint_prefix: tuple[str, ...] = ()


class PackageManager(Protocol):
    """Prepare declarative inputs without running installations on the host."""

    def prepare(
        self,
        task_sources: tuple[Path, ...],
        platform: str | None,
        preceding: tuple[ManagerOutput, ...],
        /,
    ) -> ManagerOutput:
        """Return build and runtime inputs for these tasks."""
        ...


@dataclass(frozen=True)
class Uv:
    """Prepare a frozen uv project, workspace, or PEP 723 script."""

    dependency_groups: tuple[str, ...] | None = None
    extras: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        """Normalize dependency selectors.

        Raises:
            ValueError: If selectors are empty or not strings.

        """
        for name in ("dependency_groups", "extras"):
            values = getattr(self, name)
            if values is not None:
                if any(not isinstance(value, str) or not value for value in values):
                    msg = f"{name} must contain non-empty strings"
                    raise ValueError(msg)
                object.__setattr__(self, name, tuple(sorted(set(values))))

    def prepare(
        self,
        task_sources: tuple[Path, ...],
        _platform: str | None,
        preceding: tuple[ManagerOutput, ...],
    ) -> ManagerOutput:
        """Resolve uv selections and generate its installation recipe.

        Returns:
            The frozen dependency recipe and selected source layout.

        Raises:
            ValueError: If preceding managers conflict over Python ownership.

        """
        source = prepare_source(
            task_sources, dependency_groups=self.dependency_groups, extras=self.extras
        )
        owners = [output.python for output in preceding if output.python is not None]
        if len(owners) > 1:
            msg = "conflicting Python interpreter ownership"
            raise ValueError(msg)
        python = owners[0] if owners else source.python_requires
        env = {
            "VIRTUAL_ENV": "/opt/lutra/venv",
            "UV_PYTHON_INSTALL_DIR": "/opt/lutra/python",
            "UV_CACHE_DIR": "/opt/lutra/cache",
            "UV_LINK_MODE": "copy",
            "PYTHONPATH": ":".join("/workspace/" + path for path in source.python_paths),
        }
        if owners:
            env["UV_PYTHON_DOWNLOADS"] = "never"
        files = {"uv/" + name: contents for name, contents in source.build_files.items()}
        sync = ["/usr/local/bin/uv", *source.sync_args[1:]]
        sync[sync.index("--script") + 1] = "/opt/lutra/dependencies/uv/environment.py"
        if owners:
            env["UV_PYTHON"] = python
            sync.extend(("--python", python))
        files["uv/sync.sh"] = (shlex.join(sync) + "\n").encode()
        runtime = {".lutra/uv/" + name: contents for name, contents in source.runtime_files.items()}
        # Local lock paths are relative to the generated script's directory.
        if runtime:
            locked = tomllib.loads(runtime[".lutra/uv/lutra-runtime.py.lock"].decode())
            runtime[".lutra/uv/lutra-runtime.py.lock"] = _native_lock(
                locked, {location: "../../" + location for _, location in _local_sources(locked)}
            )
        prepare_commands: tuple[tuple[str, ...], ...] = ()
        if runtime:
            prepare_args = list(source.prepare_args)
            prepare_args[0] = "/usr/local/bin/uv"
            prepare_args[prepare_args.index("--script") + 1] = (
                "/workspace/.lutra/uv/lutra-runtime.py"
            )
            prepare_commands = (tuple(prepare_args),)
        commands = (
            ("/usr/local/bin/uv", "venv", "--python", python, "/opt/lutra/venv"),
            ("sh", "/opt/lutra/dependencies/uv/sync.sh"),
        )
        layout = source.layout
        layout = replace(layout, runtime_files=runtime)
        prefix = ("/opt/lutra/venv/bin/python", "-m", "lutra.serve")
        return ManagerOutput(
            files,
            runtime,
            (OciCopy(UV_IMAGE, "/uv", "/usr/local/bin/uv"),),
            commands,
            prepare_commands,
            env,
            source=layout,
            entrypoint_prefix=prefix,
        )


MISE_IMAGE = "ghcr.io/jdx/mise:2026.10.0"


@dataclass(frozen=True)
class Mise:
    """Install pinned core tools from one explicit mise configuration."""

    config: str

    def prepare(
        self,
        task_sources: tuple[Path, ...],
        _platform: str | None,
        preceding: tuple[ManagerOutput, ...],
    ) -> ManagerOutput:
        """Bundle configuration and install tools without shell activation.

        Returns:
            Isolated mise installation and execution declarations.

        Raises:
            ValueError: If configuration or Python ownership is unsupported.

        """
        if not task_sources:
            msg = "an environment must declare at least one task"
            raise ValueError(msg)
        root = (
            task_sources[0].parent
            if _script_metadata(task_sources[0]) is not None
            else _owner(task_sources[0])[0]
        )
        relative = Path(self.config)
        if relative.is_absolute() or ".." in relative.parts:
            msg = "mise config must be a relative path inside the task project"
            raise ValueError(msg)
        config = _safe_path(root / relative)
        contents = config.read_bytes()
        data = tomllib.loads(contents.decode())
        tools = data.get("tools", {})
        if set(data) != {"tools"} or not tools:
            msg = "mise supports only a non-empty tools table"
            raise ValueError(msg)
        for tool, version in tools.items():
            if (
                tool not in {"python", "node", "go", "ruby", "java", "bun", "deno", "rust", "zig"}
                or not isinstance(version, str)
                or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version)
            ):
                msg = "mise tools must be supported core tools with exact pinned versions"
                raise ValueError(msg)
        if "python" in tools and any(output.python or output.source for output in preceding):
            msg = "mise Python must precede uv"
            raise ValueError(msg)
        binary = "/usr/local/bin/mise"
        selections = tuple(f"core:{tool}@{version}" for tool, version in sorted(tools.items()))
        prefix = (binary, "--no-config", "exec", *selections, "--")
        env = {
            "MISE_DATA_DIR": "/opt/lutra/mise/data",
            "MISE_CACHE_DIR": "/opt/lutra/mise/cache",
            "MISE_CONFIG_DIR": "/opt/lutra/mise/config",
            "MISE_GLOBAL_CONFIG_FILE": "/dev/null",
            "MISE_SYSTEM_CONFIG_DIR": "/opt/lutra/mise/system",
            "MISE_CEILING_PATHS": "/opt/lutra/dependencies/mise",
            "MISE_OVERRIDE_CONFIG_FILENAMES": "mise.toml",
            "MISE_OVERRIDE_TOOL_VERSIONS_FILENAMES": "none",
            "MISE_NO_HOOKS": "1",
            "MISE_NO_ENV": "1",
            "MISE_YES": "1",
        }
        return ManagerOutput(
            build_files={"mise/mise.toml": contents},
            oci_copies=(OciCopy(MISE_IMAGE, "/usr/local/bin/mise", binary),),
            build_commands=((binary, "--no-config", "install", *selections),),
            build_env=env,
            wrapper=prefix,
            python=("/opt/lutra/mise/data/installs/python/" + tools["python"] + "/bin/python")
            if "python" in tools
            else None,
        )


def wrap(command: tuple[str, ...], outputs: tuple[ManagerOutput, ...]) -> tuple[str, ...]:
    """Apply wrappers with the first manager outermost.

    Returns:
        The wrapped argv command.

    """
    for output in reversed(outputs):
        command = output.wrapper + command
    return command


def prepare_managers(
    managers: tuple[PackageManager, ...], task_sources: tuple[Path, ...], platform: str | None
) -> tuple[ManagerOutput, ...]:
    """Compose managers and reject conflicting declarations.

    Returns:
        Outputs in declaration order.

    Raises:
        ValueError: If managers or their output declarations conflict.

    """
    if sum(isinstance(manager, Uv) for manager in managers) != 1:
        msg = "Python tasks require exactly one Uv manager"
        raise ValueError(msg)
    if len({type(manager) for manager in managers}) != len(managers):
        msg = "duplicate package managers"
        raise ValueError(msg)
    outputs: list[ManagerOutput] = []
    files: dict[str, bytes] = {}
    runtime_files: dict[str, bytes] = {}
    env: dict[str, str] = {}
    for manager in managers:
        output = manager.prepare(task_sources, platform, tuple(outputs))
        _merge(files, output.build_files)
        _merge(runtime_files, output.runtime_files)
        _merge(env, output.build_env)
        outputs.append(output)
    return tuple(outputs)


V = TypeVar("V")


def _merge(target: dict[str, V], additions: dict[str, V]) -> None:
    for key, value in additions.items():
        if key in target and target[key] != value:
            msg = f"conflicting manager declaration: {key}"
            raise ValueError(msg)
        target[key] = value
