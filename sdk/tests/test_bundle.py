"""Project file selection and locked dependency preparation."""

import os
import runpy
import shutil
import subprocess  # ruff: ignore[suspicious-subprocess-import]
from pathlib import Path
from unittest.mock import AsyncMock

import cbor2
import pytest
from lutra._bundle import build_bundle, select_files
from lutra._dependency import discover_source
from lutra.archive import extract_archive, read_archive
from lutra.client import _BundleInputs, _prepare_bundle
from lutra.serve.__main__ import _bundled_handler, _load_entrypoint
from lutra.task import TaskEnvironment, TaskImage
from lutra.value import dumps


def _uv_lock(root: Path, *args: str) -> None:
    uv = shutil.which("uv")
    assert uv is not None
    subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
        [uv, "lock", *args], cwd=root, check=True, capture_output=True
    )


def _identity(value: int) -> int:
    return value


def test_bundle_includes_assets_and_honors_nested_ignores(tmp_path: Path) -> None:
    (tmp_path / ".gitignore").write_text("*.json\nprivate/\n", encoding="utf-8")
    (tmp_path / "config.json").write_text("secret", encoding="utf-8")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / ".gitignore").write_text("!keep.json\n", encoding="utf-8")
    (tmp_path / "src" / "keep.json").write_text('{"value": 1}', encoding="utf-8")
    (tmp_path / "src" / "omit.json").write_text("omit", encoding="utf-8")
    (tmp_path / "src" / "task.py").write_text("value = 1\n", encoding="utf-8")
    (tmp_path / "private").mkdir()
    (tmp_path / "private" / ".gitignore").write_text("!key.txt\n", encoding="utf-8")
    (tmp_path / "private" / "key.txt").write_text("secret", encoding="utf-8")
    (tmp_path / ".venv").mkdir()
    (tmp_path / ".venv" / "python").write_text("binary", encoding="utf-8")

    selected = select_files(tmp_path)
    assert Path("src/keep.json") in selected
    assert Path("src/task.py") in selected
    assert Path("src/omit.json") not in selected
    assert Path("config.json") not in selected
    assert Path("private/key.txt") not in selected
    assert Path("private/key.txt") in select_files(tmp_path, includes=(Path("private/key.txt"),))
    assert Path(".venv/python") not in selected

    first = build_bundle(tmp_path)
    assert first == build_bundle(tmp_path)
    archive = tmp_path / "source.tar.zst"
    archive.write_bytes(first)
    assert read_archive(archive, "src/keep.json") == b'{"value": 1}'


def test_explicit_include_overrides_gitignore_but_not_safety_exclusions(tmp_path: Path) -> None:
    (tmp_path / ".gitignore").write_text("generated/\n", encoding="utf-8")
    (tmp_path / "generated").mkdir()
    (tmp_path / "generated" / "binding.py").write_text("value = 1\n", encoding="utf-8")
    assert Path("generated/binding.py") not in select_files(tmp_path)
    assert Path("generated/binding.py") in select_files(tmp_path, includes=(Path("generated"),))
    with pytest.raises(ValueError, match="excluded"):
        select_files(tmp_path, includes=(Path(".venv"),))


def test_lutraignore_and_glob_includes(tmp_path: Path) -> None:
    (tmp_path / ".gitignore").write_text("*.json\n", encoding="utf-8")
    (tmp_path / ".lutraignore").write_text("hidden.py\n", encoding="utf-8")
    source = tmp_path / "src"
    source.mkdir()
    (source / ".lutraignore").write_text("!keep.json\n", encoding="utf-8")
    for name in ("keep.json", "omit.json", "hidden.py", "task.py"):
        (source / name).write_text(name, encoding="utf-8")
    (tmp_path / ".venv").mkdir()
    (tmp_path / ".venv" / "hidden.py").write_text("private", encoding="utf-8")

    assert select_files(tmp_path) == (
        Path(".gitignore"),
        Path(".lutraignore"),
        Path("src/.lutraignore"),
        Path("src/keep.json"),
        Path("src/task.py"),
    )
    selected = select_files(tmp_path, includes=(Path("src/*.py"),))
    assert Path("src/hidden.py") in selected
    assert Path(".venv/hidden.py") not in selected
    with pytest.raises(ValueError, match="has no files"):
        select_files(tmp_path, includes=(Path("src/*.csv"),))


def test_bundle_rejects_symlink_and_external_include(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside.py"
    (tmp_path / "link.py").symlink_to(outside)
    with pytest.raises(ValueError, match="not a regular file"):
        select_files(tmp_path)
    (tmp_path / "link.py").unlink()
    with pytest.raises(ValueError, match="inside"):
        select_files(tmp_path, includes=(Path("../outside.py"),))


def test_scoped_selection_respects_ignored_parent(tmp_path: Path) -> None:
    (tmp_path / ".gitignore").write_text("library/\n", encoding="utf-8")
    library = tmp_path / "library"
    library.mkdir()
    (library / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    assert select_files(tmp_path, scopes=(library,)) == ()
    assert select_files(tmp_path, scopes=(library,), includes=(Path("library/module.py"),)) == (
        Path("library/module.py"),
    )


def test_scoped_selection_adds_includes_outside_package(tmp_path: Path) -> None:
    package = tmp_path / "package"
    package.mkdir()
    (package / "task.py").write_text("VALUE = 1\n", encoding="utf-8")
    assets = tmp_path / "assets"
    assets.mkdir()
    (assets / "banner.html").write_text("<h1>Hello</h1>", encoding="utf-8")
    (assets / "other.html").write_text("<h1>Other</h1>", encoding="utf-8")
    (tmp_path / ".lutraignore").write_text("assets/\n", encoding="utf-8")

    assert select_files(tmp_path, scopes=(package,), includes=(Path("assets/banner.html"),)) == (
        Path("assets/banner.html"),
        Path("package/task.py"),
    )
    assert select_files(tmp_path, scopes=(package,), includes=(Path("assets"),)) == (
        Path("assets/banner.html"),
        Path("assets/other.html"),
        Path("package/task.py"),
    )


def _environment(
    monkeypatch: pytest.MonkeyPatch,
    task_file: Path,
    *,
    image: TaskImage | None = None,
    source_includes: tuple[Path, ...] = (),
) -> TaskEnvironment:
    environment = TaskEnvironment(
        "bundle", image=image or TaskImage(), source_includes=source_includes
    )
    task = environment.task(_identity)
    monkeypatch.setattr(task, "source_file", task_file)
    return environment


def _archives(tmp_path: Path, environment: TaskEnvironment) -> tuple[_BundleInputs, Path, Path]:
    inputs = _prepare_bundle(environment)
    source_archive = tmp_path / "source.tar.zst"
    source_archive.write_bytes(inputs.source)
    build_archive = tmp_path / "build.tar.zst"
    build_archive.write_bytes(inputs.build_context)
    return inputs, source_archive, build_archive


def _sync_archive(tmp_path: Path, inputs: _BundleInputs, archive: Path) -> Path:
    destination = tmp_path / "installed"
    extract_archive(archive, destination)
    project = destination / inputs.workdir
    venv = project / ".venv"
    uv = shutil.which("uv")
    assert uv is not None
    subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
        [uv, "venv", str(venv)], check=True, capture_output=True
    )
    result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
        [*inputs.build_command, "--offline"],
        cwd=project,
        check=False,
        capture_output=True,
        env={**os.environ, "UV_PROJECT_ENVIRONMENT": str(venv), "VIRTUAL_ENV": str(venv)},
    )
    assert result.returncode == 0, result.stderr.decode()
    return destination


def test_workspace_packages_and_cache_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / ".gitignore").write_text("*.json\n", encoding="utf-8")
    project = tmp_path / "project"
    project.mkdir()
    (project / "pyproject.toml").write_text(
        '[project]\nname = "app"\nversion = "0.1.0"\n'
        'requires-python = ">=3.11"\ndependencies = ["library"]\n'
        '[tool.uv.workspace]\nmembers = ["../library"]\n'
        "[tool.uv.sources]\nlibrary = { workspace = true }\n",
        encoding="utf-8",
    )
    library = tmp_path / "library"
    package = library / "src" / "library"
    package.mkdir(parents=True)
    (library / "pyproject.toml").write_text(
        '[project]\nname = "library"\nversion = "0.1.0"\nrequires-python = ">=3.11"\n',
        encoding="utf-8",
    )
    (library / ".gitignore").write_text("*.json\n", encoding="utf-8")
    (package / "__init__.py").write_text("VALUE = 3\n", encoding="utf-8")
    (package / "asset.json").write_text('{"value": 3}', encoding="utf-8")
    (package / "hidden.json").write_text("private", encoding="utf-8")
    (project / "task.py").write_text("value = 1\n", encoding="utf-8")
    _uv_lock(project, "--offline")
    environment = _environment(
        monkeypatch, project / "task.py", source_includes=(library / "src/library/asset.json",)
    )
    source = discover_source(project / "task.py")
    assert source.bundle_root == tmp_path
    assert Path("library/src") in source.import_roots
    first, source_archive, build_archive = _archives(tmp_path, environment)
    assert read_archive(source_archive, "library/src/library/asset.json") == b'{"value": 3}'
    with pytest.raises(KeyError):
        read_archive(source_archive, "library/src/library/hidden.json")
    assert read_archive(build_archive, "project/uv.lock") == (project / "uv.lock").read_bytes()
    assert (
        read_archive(build_archive, "library/pyproject.toml")
        == (library / "pyproject.toml").read_bytes()
    )
    assert first.workdir == "project"
    assert first.build_command == (
        "uv",
        "sync",
        "--frozen",
        "--active",
        "--all-packages",
        "--no-dev",
        "--no-install-local",
    )
    destination = _sync_archive(tmp_path, first, build_archive)
    extract_archive(source_archive, destination)
    subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
        [
            str(destination / "project/.venv/bin/python"),
            "-c",
            "import library; assert library.VALUE == 3",
        ],
        check=True,
        capture_output=True,
        env={**os.environ, "PYTHONPATH": str(destination / "library/src")},
    )
    (project / "task.py").write_text("value = 2\n", encoding="utf-8")
    changed, _, _ = _archives(tmp_path, environment)
    assert changed.source != first.source
    assert changed.build_context == first.build_context
    (library / "pyproject.toml").write_text(
        (library / "pyproject.toml").read_text().replace("0.1.0", "0.2.0"), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="lock check failed"):
        _prepare_bundle(environment)
    _uv_lock(project, "--offline")
    changed, _, _ = _archives(tmp_path, environment)
    assert changed.build_context != first.build_context


def test_source_includes_relative_to_declaring_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "app"\nversion = "0.1.0"\nrequires-python = ">=3.11"\n', encoding="utf-8"
    )
    task_file = tmp_path / "task.py"
    task_file.write_text("value = 1\n", encoding="utf-8")
    declaration = tmp_path / "pkg" / "environment.py"
    declaration.parent.mkdir()
    declaration.write_text(
        "from pathlib import Path\n"
        "from lutra import TaskEnvironment\n"
        "environment = TaskEnvironment('bundle', source_includes=(Path('templates/*.html'),))\n",
        encoding="utf-8",
    )
    templates = declaration.parent / "templates"
    templates.mkdir()
    (templates / "banner.html").write_text("<h1>Hello</h1>", encoding="utf-8")
    (tmp_path / ".lutraignore").write_text("*.html\n", encoding="utf-8")
    _uv_lock(tmp_path, "--offline")
    environment = runpy.run_path(str(declaration))["environment"]
    assert isinstance(environment, TaskEnvironment)
    task = environment.task(_identity)
    monkeypatch.setattr(task, "source_file", task_file)

    _, archive, _ = _archives(tmp_path, environment)
    assert read_archive(archive, "pkg/templates/banner.html") == b"<h1>Hello</h1>"


def test_sibling_package_requires_shared_repository(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    library = tmp_path / "library"
    project.mkdir()
    library.mkdir()
    (project / "task.py").write_text("value = 1\n", encoding="utf-8")
    (project / "pyproject.toml").write_text(
        '[project]\nname = "app"\nversion = "0.1.0"\nrequires-python = ">=3.11"\n'
        'dependencies = ["library"]\n[tool.uv.sources]\nlibrary = { path = "../library" }\n',
        encoding="utf-8",
    )
    (library / "pyproject.toml").write_text(
        '[project]\nname = "library"\nversion = "0.1.0"\nrequires-python = ">=3.11"\n',
        encoding="utf-8",
    )
    _uv_lock(project, "--offline")
    environment = _environment(monkeypatch, project / "task.py")
    with pytest.raises(ValueError, match="leaves repository"):
        _prepare_bundle(environment)
    (tmp_path / ".git").mkdir()
    source = discover_source(project / "task.py")
    assert library in source.local_roots
    assert source.bundle_root == tmp_path
    (library / ".git").mkdir()
    with pytest.raises(ValueError, match="leaves repository"):
        discover_source(project / "task.py")


def test_script_metadata_and_body_have_separate_cache_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "task.py"
    script.write_text(
        '# /// script\n# requires-python = ">=3.11"\n# dependencies = []\n# ///\nvalue = 1\n',
        encoding="utf-8",
    )
    environment = _environment(monkeypatch, script)
    with pytest.raises(ValueError, match="sidecar lock"):
        _prepare_bundle(environment)
    _uv_lock(tmp_path, "--script", str(script), "--offline")
    first, _, build_archive = _archives(tmp_path, environment)
    assert read_archive(build_archive, "task.py") == (
        b'# /// script\n# requires-python = ">=3.11"\n# dependencies = []\n# ///\n'
    )
    assert first.build_command[:4] == ("uv", "sync", "--script", "task.py")
    _sync_archive(tmp_path, first, build_archive)
    script.write_text(script.read_text().replace("value = 1", "value = 2"), encoding="utf-8")
    changed, _, _ = _archives(tmp_path, environment)
    assert changed.source != first.source
    assert changed.build_context == first.build_context
    script.write_text(script.read_text().replace(">=3.11", ">=3.12"), encoding="utf-8")
    with pytest.raises(ValueError, match="lock check failed"):
        _prepare_bundle(environment)
    _uv_lock(tmp_path, "--script", str(script), "--offline")
    changed, _, _ = _archives(tmp_path, environment)
    assert changed.build_context != first.build_context


def test_custom_build_context_and_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "app"\nversion = "0.1.0"\nrequires-python = ">=3.11"\n', encoding="utf-8"
    )
    (tmp_path / "task.py").write_text("value = 1\n", encoding="utf-8")
    (tmp_path / "extra").mkdir()
    (tmp_path / "extra" / "build.txt").write_text("input", encoding="utf-8")
    _uv_lock(tmp_path, "--offline")
    image = TaskImage(build_context=Path("extra"), build_command=("true",))
    environment = _environment(monkeypatch, tmp_path / "task.py", image=image)
    inputs, _, build_archive = _archives(tmp_path, environment)
    assert inputs.build_command == ("true",)
    assert read_archive(build_archive, "extra/build.txt") == b"input"
    assert (
        read_archive(build_archive, "pyproject.toml") == (tmp_path / "pyproject.toml").read_bytes()
    )
    with pytest.raises(ValueError, match="conflicts"):
        _prepare_bundle(
            _environment(monkeypatch, tmp_path / "task.py", image=TaskImage(build_context=Path()))
        )
    with pytest.raises(ValueError, match="inside the locked source root"):
        _prepare_bundle(
            _environment(
                monkeypatch, tmp_path / "task.py", image=TaskImage(build_context=Path("../outside"))
            )
        )
    nested = _environment(monkeypatch, tmp_path / "task.py", image=TaskImage(workdir="run"))
    nested_inputs, _, nested_archive = _archives(tmp_path, nested)
    assert nested_inputs.build_command[-2:] == ("--project", "..")
    _sync_archive(tmp_path, nested_inputs, nested_archive)


@pytest.mark.asyncio
async def test_pep723_file_entrypoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = tmp_path / "task.py"
    script.write_text(
        '# /// script\n# requires-python = ">=3.11"\n# dependencies = []\n# ///\n'
        "from lutra import TaskEnvironment\n"
        'environment = TaskEnvironment("script")\n'
        "@environment.task\n"
        "def double(value: int) -> int:\n"
        "    return 2 * value\n"
        'if __name__ == "__main__":\n'
        '    raise RuntimeError("script main ran")\n',
        encoding="utf-8",
    )
    _uv_lock(tmp_path, "--script", str(script), "--offline")
    monkeypatch.chdir(tmp_path)
    task = _load_entrypoint("file:task.py:double")
    assert task.function(3) == 6
    monkeypatch.setenv("LUTRA_ENVIRONMENTS_JSON", "[]")
    handler, _ = _bundled_handler("file:task.py:double")
    mime, result = await handler("run", "application/cbor", dumps([[3], {}]), AsyncMock())
    assert mime == "application/cbor"
    assert cbor2.loads(result) == 6


def test_nested_file_entrypoint_uses_bundle_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".git").mkdir()
    project = tmp_path / "project"
    project.mkdir()
    script = project / "task.py"
    script.write_text(
        '# /// script\n# requires-python = ">=3.11"\n# dependencies = []\n# ///\n'
        "from lutra import TaskEnvironment\n"
        'environment = TaskEnvironment("nested")\n'
        "@environment.task\n"
        "def double(value: int) -> int:\n"
        "    return value * 2\n",
        encoding="utf-8",
    )
    _uv_lock(project, "--script", str(script), "--offline")
    monkeypatch.chdir(project)
    monkeypatch.setenv("LUTRA_BUNDLE_ROOT", str(tmp_path))
    assert _load_entrypoint("file:project/task.py:double").function(3) == 6
