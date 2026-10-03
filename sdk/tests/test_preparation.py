"""Real uv preparation, source selection and editable installation contracts."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
import tomllib
from pathlib import Path
from typing import Literal

import pytest
from lutra._bundle import bundle_bytes, select_files
from lutra._dependency import UV_VERSION, prepare_source
from lutra._source_bundle import build_source_bundle
from lutra.archive import extract_archive


def project(
    root: Path,
    name: str,
    *,
    extra: str = "",
    layout: str = "src",
    backend: Literal["uv_build", "hatchling.build"] | None = "uv_build",
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    build = ""
    if backend is not None:
        # uv's bundled backend avoids repeated isolated Python builds for fixtures.
        requirement = f"uv_build=={UV_VERSION}" if backend == "uv_build" else "hatchling"
        build = f'\n[build-system]\nrequires=["{requirement}"]\nbuild-backend="{backend}"\n'
        if backend == "uv_build":
            build += f'\n[tool.uv.build-backend]\nmodule-root="{layout}"\n'
    (root / "pyproject.toml").write_text(
        f'[project]\nname="{name}"\nversion="1.2.3"\nrequires-python=">=3.11"\n{extra}{build}'
    )
    package = root / layout / name if layout else root / name
    package.mkdir(parents=True)
    (package / "__init__.py").write_text('VALUE = "first"\ndef main(): print(VALUE)\n')
    task = root / "tasks.py"
    task.write_text(
        "from lutra import TaskEnvironment\n"
        'env = TaskEnvironment("test")\n@env.task\ndef work(): return 1\n'
    )
    return task


def unpack(tmp_path: Path, contents: bytes, name: str = "source") -> Path:
    archive = tmp_path / f"{name}.tar.zst"
    archive.write_bytes(contents)
    root = tmp_path / name
    extract_archive(archive, root)
    return root


def run(*args: str, cwd: Path | None = None, env: dict[str, str] | None = None) -> str:
    return subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
        args, cwd=cwd, env=env, check=True, capture_output=True, text=True
    ).stdout


def sync_source(files: Path, env: Path) -> None:
    run("uv", "venv", "--python", sys.executable, str(env))
    run(
        "uv",
        "sync",
        "--frozen",
        "--active",
        "--no-config",
        "--script",
        str(files / "lutra-runtime.py"),
        env={**os.environ, "VIRTUAL_ENV": str(env)},
    )


def selected_names(script: Path) -> set[str]:
    graph = json.loads(
        run("uv", "tree", "--frozen", "--universal", "--format", "json", "--script", str(script))
    )
    return {p["name"] for p in graph["resolution"].values() if p.get("kind") == "package"}


@pytest.mark.parametrize("script", [False, True])
def test_decoration_has_no_uv_side_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, script: bool
) -> None:
    task = project(tmp_path, "demo", backend=None)
    if script:
        task.write_text(
            '# /// script\n# requires-python=">=3.11"\n# dependencies=["lutra"]\n# ///\n'
            + task.read_text()
        )
    monkeypatch.setenv("PATH", "")
    spec = importlib.util.spec_from_file_location("tasks", task)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert not (tmp_path / "uv.lock").exists()
    assert not Path(f"{task}.lock").exists()
    assert module.env.tasks[0].source_file == task


def test_missing_stale_locks_and_dependency_identity(tmp_path: Path) -> None:
    task = project(tmp_path / "project", "demo", backend=None)
    first = prepare_source([task])
    assert first.lock.exists()
    original = first.lock.read_bytes()
    first_bundle = build_source_bundle(first, [task.relative_to(first.bundle_root)])
    task.write_text(task.read_text() + "\n# source changed\n")
    manifest = task.parent / "pyproject.toml"
    manifest.write_text(manifest.read_text() + "\n[tool.editor]\nwidth=90\n")
    second = prepare_source([task])
    assert second.build_files == first.build_files
    assert build_source_bundle(second, [task.relative_to(second.bundle_root)]) != first_bundle
    second.lock.write_bytes(original + b"\n# lock changed\n")
    assert prepare_source([task]).build_files != first.build_files
    manifest.write_text(manifest.read_text().replace('version="1.2.3"', 'version="1.2.4"'))
    with pytest.raises(ValueError, match="run `uv lock --project"):
        prepare_source([task])
    assert second.lock.read_bytes() == original + b"\n# lock changed\n"


def test_workspace_selection_and_virtual_root(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text('[tool.uv.workspace]\nmembers=["a", "b", "unused"]\n')
    a = project(
        tmp_path / "a", "a", extra='dependencies=["b"]\n[tool.uv.sources]\nb={workspace=true}\n'
    )
    b = project(tmp_path / "b", "b")
    project(tmp_path / "unused", "unused")
    selected = prepare_source([a])
    assert set(selected.project_roots) == {a.parent, b.parent}
    assert prepare_source([a, b]).build_files != selected.build_files
    files = unpack(tmp_path, build_source_bundle(selected, [Path("a/tasks.py")]))
    assert (files / "a/src/a/__init__.py").exists()
    assert (files / "b/src/b/__init__.py").exists()
    assert not (files / "unused").exists()
    root_task = tmp_path / "tasks.py"
    root_task.write_text("pass\n")
    virtual = prepare_source([root_task])
    assert virtual.project_roots == (tmp_path,)
    assert not virtual.runtime_files
    root_files = unpack(tmp_path, build_source_bundle(virtual, [Path("tasks.py")]), "virtual")
    assert not (root_files / "unused").exists()
    assert not (root_files / "a").exists()


def test_adjacent_cross_repository_transitive_and_relocation(tmp_path: Path) -> None:
    root = tmp_path / "original"
    a = project(
        root / "app",
        "app",
        extra='dependencies=["shared"]\n[tool.uv.sources]\nshared={path="../shared"}\n',
    )
    shared = project(
        root / "shared",
        "shared",
        extra='dependencies=["leaf"]\n[tool.uv.sources]\nleaf={path="../leaf"}\n',
    )
    leaf = project(root / "leaf", "leaf")
    for folder in (a.parent, shared.parent, leaf.parent):
        (folder / ".git").mkdir()
    (root / "unrelated-secret").write_text("do not scan this ancestor")
    selected = prepare_source([a])
    assert set(selected.project_roots) == {a.parent, shared.parent, leaf.parent}
    assert selected.bundle_root == root
    first = build_source_bundle(selected, [Path("app/tasks.py")])
    files = unpack(tmp_path, first)
    assert not (files / "unrelated-secret").exists()
    local = tomllib.loads((files / "lutra-runtime.py.lock").read_text())
    assert {p["source"]["editable"] for p in local["package"]} == {"app", "shared", "leaf"}
    shutil.copytree(root, tmp_path / "relocated")
    relocated = prepare_source([tmp_path / "relocated/app/tasks.py"])
    assert bundle_bytes(relocated.build_files) == bundle_bytes(selected.build_files)
    assert build_source_bundle(relocated, [Path("app/tasks.py")]) == first


def test_script_local_dependency_and_independent_owners(tmp_path: Path) -> None:
    local = project(tmp_path / "local", "local")
    script_dir = tmp_path / "scripts"
    script_dir.mkdir()
    script = script_dir / "run.py"
    script.write_text(
        '# /// script\n# requires-python = ">=3.11"\n# dependencies = ["local"]\n'
        '# [tool.uv.sources]\n# local = {path="../local"}\n# [tool.lutra]\n'
        '# source-includes=["asset.txt"]\n# ///\npass\n'
    )
    (script_dir / "asset.txt").write_text("asset")
    (script_dir / "unrelated.txt").write_text("omit")
    prepared = prepare_source([script, local])
    assert prepared.lock == Path(f"{script}.lock")
    assert not (local.parent / "uv.lock").exists()
    files = unpack(
        tmp_path, build_source_bundle(prepared, [Path("scripts/run.py"), Path("local/tasks.py")])
    )
    assert (files / "scripts/asset.txt").exists()
    assert not (files / "scripts/unrelated.txt").exists()
    other = project(tmp_path / "other", "other")
    with pytest.raises(ValueError, match="share one locked"):
        prepare_source([script, other])
    with pytest.raises(ValueError, match="share one locked"):
        prepare_source([local, other])


def test_ignores_required_files_modes_and_includes(tmp_path: Path) -> None:
    task = project(tmp_path / "repo/project", "demo", backend=None)
    root = task.parent
    (root.parent / ".gitignore").write_text("project/ignored/\nproject/tasks.py\n")
    (root / "ignored").mkdir()
    (root / "ignored/keep.txt").write_text("keep")
    (root / "ignored/drop.txt").write_text("drop")
    (root / "nested").mkdir()
    (root / "nested/.gitignore").write_text("drop\n")
    (root / "nested/drop").write_text("drop")
    (root / "run").write_text("#!/bin/sh\n")
    (root / "run").chmod(0o755)
    (root / ".venv").mkdir()
    (root / ".venv/secret").write_text("excluded")
    manifest = root / "pyproject.toml"
    manifest.write_text(
        manifest.read_text() + '\n[tool.lutra]\nsource-includes=["ignored/keep.txt"]\n'
    )
    prepared = prepare_source([task])
    contents = build_source_bundle(prepared, [Path("tasks.py")])
    assert contents == build_source_bundle(prepared, [Path("tasks.py")])
    files = unpack(tmp_path, contents)
    assert (files / "ignored/keep.txt").read_text() == "keep"
    assert not (files / "ignored/drop.txt").exists()
    assert not (files / "nested/drop").exists()
    assert not (files / ".venv").exists()
    assert (files / "tasks.py").exists()
    assert (files / "run").stat().st_mode & 0o111
    (root / "link").symlink_to(task)
    with pytest.raises(ValueError, match="regular file"):
        build_source_bundle(prepared, [Path("tasks.py")])
    with pytest.raises(ValueError, match="inside"):
        select_files(root, includes=[Path("../../outside")])
    with pytest.raises(ValueError, match="no files"):
        select_files(root, scopes=(), includes=[Path("missing")])


@pytest.mark.parametrize("layout", ["src", ""])
def test_editable_metadata_entrypoints_data_and_source_edits(tmp_path: Path, layout: str) -> None:
    task = project(
        tmp_path / "project",
        "demo",
        layout=layout,
        extra='[project.scripts]\ndemo-cli="demo:main"\n',
    )
    package = task.parent / layout / "demo"
    (package / "asset.txt").write_text("package data")
    prepared = prepare_source([task])
    files = unpack(tmp_path, build_source_bundle(prepared, [Path("tasks.py")]))
    env = tmp_path / "venv"
    sync_source(files, env)
    code = (
        "import demo, importlib.metadata as m, importlib.resources as r; "
        'print(demo.VALUE, m.version("demo"), '
        'r.files("demo").joinpath("asset.txt").read_text())'
    )
    assert run(str(env / "bin/python"), "-c", code).strip() == "first 1.2.3 package data"
    assert run(str(env / "bin/demo-cli")).strip() == "first"
    (files / layout / "demo/__init__.py").write_text(
        'VALUE = "updated"\ndef main(): print(VALUE)\n'
    )
    assert run(str(env / "bin/demo-cli")).strip() == "updated"


def test_inline_sdk_example(tmp_path: Path) -> None:
    sdk = Path(__file__).parents[1]
    shutil.copytree(sdk, tmp_path / "sdk", ignore=shutil.ignore_patterns("*.lock", "__pycache__"))
    script = tmp_path / "sdk/examples/hello.py"
    prepared = prepare_source([script])
    assert json.loads(prepared.build_files["selection.json"])["uv"] == UV_VERSION
    files = unpack(
        tmp_path, build_source_bundle(prepared, [script.relative_to(prepared.bundle_root)])
    )
    assert (files / "src/lutra/_gen/lutra/v1/lutra_pb.py").exists()
    dependencies = selected_names(files / "lutra-runtime.py")
    assert {"rich", "lutra"} <= dependencies
    assert not {"pytest", "ruff"} & dependencies
    assert "--no-install-package lutra" in prepared.build_files["sync.sh"].decode()
    lock = tomllib.loads(prepared.runtime_files["lutra-runtime.py.lock"].decode())
    assert lock["manifest"]["requirements"][0]["editable"] == "."


def test_runtime_extras_markers_and_nondefault_dev_group(tmp_path: Path) -> None:
    task = project(
        tmp_path / "app",
        "app",
        extra=(
            'dependencies=["shared[feature]"]\n'
            '[dependency-groups]\nchecks=["devtool"]\n'
            '[tool.uv]\ndefault-groups=["checks"]\n'
            '[tool.uv.sources]\nshared={path="../shared"}\ndevtool={path="../devtool"}\n'
        ),
    )
    shared = project(
        tmp_path / "shared",
        "shared",
        extra=(
            "[project.optional-dependencies]\nfeature=[\"optional; sys_platform == 'linux'\"]\n"
            '[tool.uv.sources]\noptional={path="../optional"}\n'
        ),
    )
    optional = project(tmp_path / "optional", "optional")
    project(tmp_path / "devtool", "devtool")
    source = prepare_source([task])
    assert set(source.project_roots) == {
        task.parent,
        shared.parent,
        optional.parent,
        tmp_path / "devtool",
    }
    files = unpack(tmp_path, build_source_bundle(source, [Path("app/tasks.py")]))
    assert selected_names(files / "lutra-runtime.py") == {"app", "devtool", "shared", "optional"}
    env = tmp_path / "venv"
    sync_source(files, env)
    installed = json.loads(
        run(
            str(env / "bin/python"),
            "-c",
            "import importlib.metadata as m, json; "
            "print(json.dumps(sorted(d.name for d in m.distributions())))",
        )
    )
    assert installed == ["app", "devtool", "optional", "shared"]


def test_transitive_virtual_projects_are_source_only(tmp_path: Path) -> None:
    task = project(
        tmp_path / "app",
        "app",
        extra='dependencies=["virtual[feature]"]\n[tool.uv.sources]\nvirtual={path="../virtual"}\n',
    )
    virtual = project(
        tmp_path / "virtual",
        "virtual",
        backend=None,
        extra=(
            '[project.optional-dependencies]\nfeature=["leaf"]\n'
            '[tool.uv.sources]\nleaf={path="../leaf"}\n'
        ),
    )
    leaf = project(tmp_path / "leaf", "leaf", backend=None)
    for path in (virtual, leaf):
        manifest = path.parent / "pyproject.toml"
        manifest.write_text(manifest.read_text() + "\n[tool.uv]\npackage=false\n")
    source = prepare_source([task])
    assert set(source.project_roots) == {task.parent, virtual.parent, leaf.parent}
    local = tomllib.loads(source.runtime_files["lutra-runtime.py.lock"].decode())
    assert [p["name"] for p in local["package"] if "editable" in p["source"]] == ["app"]


def test_namespace_package_is_installed_by_backend(tmp_path: Path) -> None:
    # Keep an external PEP 660 backend covered, including isolated build setup.
    task = project(tmp_path / "project", "demo", backend="hatchling.build")
    manifest = task.parent / "pyproject.toml"
    manifest.write_text(
        manifest.read_text() + '\n[tool.hatch.build.targets.wheel]\npackages=["src/company"]\n'
    )
    namespace = task.parent / "src/company/team"
    namespace.mkdir(parents=True)
    (namespace / "__init__.py").write_text('VALUE = "namespace"\n')
    source = prepare_source([task])
    files = unpack(tmp_path, build_source_bundle(source, [Path("tasks.py")]))
    env = tmp_path / "venv"
    sync_source(files, env)
    assert (
        run(str(env / "bin/python"), "-c", "import company.team; print(company.team.VALUE)").strip()
        == "namespace"
    )


def test_required_files_conflicts_and_archive_limits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    task = project(tmp_path / "project", "demo")
    source = prepare_source([task])
    (task.parent / "lutra-runtime.py.lock").write_text("conflicting user data")
    with pytest.raises(ValueError, match="conflicting bundle contents"):
        build_source_bundle(source, [Path("tasks.py")])
    with pytest.raises(ValueError, match="unsafe"):
        bundle_bytes({"../escape": b"no"})
    with pytest.raises(ValueError, match="conflicting"):
        bundle_bytes({"file": b"data", "file/child": b"data"})
    monkeypatch.setattr("lutra._bundle._MAX_FILE_SIZE", 2)
    with pytest.raises(ValueError, match="size limit"):
        select_files(task.parent)
    monkeypatch.setattr("lutra._bundle._MAX_FILE_SIZE", 128 << 20)
    monkeypatch.setattr("lutra._bundle._MAX_FILES", 1)
    with pytest.raises(ValueError, match="too many files"):
        select_files(task.parent)
    task.rename(task.with_suffix(".missing"))
    with pytest.raises(ValueError, match="no files"):
        build_source_bundle(source, [Path("tasks.py")])


def test_workspace_exclusion_owns_its_lock(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[tool.uv.workspace]\nmembers=["*"]\nexclude=["standalone"]\n'
    )
    task = project(tmp_path / "standalone", "standalone", backend=None)
    source = prepare_source([task])
    assert source.root == task.parent
    assert source.lock == task.parent / "uv.lock"
    assert not (tmp_path / "uv.lock").exists()


def test_symlinked_task_source_is_rejected(tmp_path: Path) -> None:
    task = project(tmp_path / "project", "demo", backend=None)
    link = task.parent / "linked.py"
    link.symlink_to(task)
    spec = importlib.util.spec_from_file_location("linked", link)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    with pytest.raises(ValueError, match="symlink"):
        spec.loader.exec_module(module)
