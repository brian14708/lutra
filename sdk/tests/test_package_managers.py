"""Adapter composition and registration behavior."""

from __future__ import annotations

import runpy
from dataclasses import dataclass
from pathlib import Path

import pytest
from lutra import Mise, TaskEnvironment, Uv
from lutra._bundle import bundle_bytes
from lutra._source_bundle import build_source_bundle
from lutra.package_managers import ManagerOutput, prepare_managers, wrap

from tests.test_preparation import project, unpack


def test_uv_generates_generic_inputs(tmp_path: Path) -> None:
    task = project(tmp_path, "demo", backend=None)
    outputs = prepare_managers((Uv(),), (task,), "linux/amd64")
    output = outputs[0]
    assert output.oci_copies[0].source == "/uv"
    assert output.build_commands[0] == (
        "/usr/local/bin/uv",
        "venv",
        "--python",
        ">=3.11",
        "/opt/lutra/venv",
    )
    assert output.entrypoint_prefix == ("/opt/lutra/venv/bin/python", "-m", "lutra.serve")
    assert set(output.build_files) == {
        "uv/environment.py",
        "uv/environment.py.lock",
        "uv/sync.sh",
        "uv/uv.lock",
        "uv/selection.json",
    }
    assert output.build_env["PYTHONPATH"] == "/workspace/."
    assert output.source is not None
    source = unpack(tmp_path, build_source_bundle(output.source, [Path("tasks.py")]))
    assert (source / "tasks.py").exists()


def test_mise_configuration_and_interpreter_ownership(tmp_path: Path) -> None:
    task = project(tmp_path, "demo", backend=None)
    config = tmp_path / "mise.toml"
    config.write_text('[tools]\npython="3.12.11"\nnode="22.16.0"\n')
    mise, uv = prepare_managers((Mise("mise.toml"), Uv()), (task,), "linux/amd64")
    assert mise.build_commands == (
        (
            "/usr/local/bin/mise",
            "--no-config",
            "install",
            "core:node@22.16.0",
            "core:python@3.12.11",
        ),
    )
    assert uv.build_env["UV_PYTHON_DOWNLOADS"] == "never"
    assert uv.build_commands[0][3] == "/opt/lutra/mise/data/installs/python/3.12.11/bin/python"
    assert wrap(("task",), (mise, uv)) == (
        "/usr/local/bin/mise",
        "--no-config",
        "exec",
        "core:node@22.16.0",
        "core:python@3.12.11",
        "--",
        "task",
    )
    first = bundle_bytes(mise.build_files)
    config.write_text('[tools]\nnode="22.17.0"\n')
    revised = Mise("mise.toml").prepare((task,), None, ())
    assert bundle_bytes(revised.build_files) != first
    uv_owned = prepare_managers((Uv(), Mise("mise.toml")), (task,), None)
    assert "UV_PYTHON_DOWNLOADS" not in uv_owned[0].build_env
    config.write_text('[tools]\npython="3.12.11"\n')
    with pytest.raises(ValueError, match="must precede uv"):
        prepare_managers((Uv(), Mise("mise.toml")), (task,), None)


@pytest.mark.parametrize(
    "text",
    [
        '[tools]\nnode="latest"\n',
        '[tools]\nnode="22.16"\n',
        '[tools]\nnode="{{env.VERSION}}"\n',
        '[tools]\ncustom="1.2.3"\n',
        '[tools]\nnode={version="22.16.0"}\n',
        '[tools]\nnode="22.16.0"\n[hooks]\nenter="echo hi"\n',
        'includes=["../outside.toml"]\n[tools]\nnode="22.16.0"\n',
    ],
)
def test_mise_rejects_unsupported_config(tmp_path: Path, text: str) -> None:
    task = project(tmp_path, "demo", backend=None)
    (tmp_path / "mise.toml").write_text(text)
    with pytest.raises(ValueError, match="mise"):
        Mise("mise.toml").prepare((task,), None, ())


def test_manager_validation_precedes_tool_execution(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exactly one"):
        prepare_managers((), (), None)
    with pytest.raises(ValueError, match="duplicate"):
        prepare_managers((Mise("a"), Mise("b"), Uv()), (), None)
    with pytest.raises(ValueError, match="exactly one"):
        prepare_managers((Uv(), Uv()), (), None)
    with pytest.raises(ValueError, match="non-empty"):
        Uv(extras=("",))
    task = project(tmp_path, "demo", backend=None)
    with pytest.raises(ValueError, match="relative path"):
        Mise("../mise.toml").prepare((task,), None, ())


@dataclass
class First:
    output: ManagerOutput

    def prepare(
        self,
        _sources: tuple[Path, ...],
        _platform: str | None,
        _preceding: tuple[ManagerOutput, ...],
    ) -> ManagerOutput:
        return self.output


class Second(First):
    pass


@pytest.mark.parametrize("field", ["build_env", "build_files", "runtime_files"])
def test_composition_rejects_conflicts(tmp_path: Path, field: str) -> None:
    first = ManagerOutput()
    second = ManagerOutput()
    if field == "build_env":
        first.build_env["VALUE"] = "a"
        second.build_env["VALUE"] = "b"
    else:
        getattr(first, field)["custom/file"] = b"a"
        getattr(second, field)["custom/file"] = b"b"
    task = project(tmp_path, "demo", backend=None)
    with pytest.raises(ValueError, match="conflicting manager"):
        prepare_managers((First(first), Second(second), Uv()), (task,), None)


def test_wrapping_and_preceding_outputs(tmp_path: Path) -> None:
    task = project(tmp_path, "demo", backend=None)
    outputs = prepare_managers(
        (First(ManagerOutput(wrapper=("outer",))), Second(ManagerOutput(wrapper=("inner",))), Uv()),
        (task,),
        None,
    )
    assert wrap(("command", "arg"), outputs) == ("outer", "inner", "command", "arg")


def test_declaring_mise_has_no_installation_side_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    task = project(tmp_path, "demo", backend=None)
    task.write_text(
        "from lutra import TaskEnvironment, Mise, Uv\n"
        'env=TaskEnvironment("test", package_managers=(Mise("missing.toml"), Uv()))\n'
        "@env.task\ndef work(): return 1\n"
    )
    monkeypatch.setenv("PATH", "")
    environment = runpy.run_path(str(task))["env"]
    assert isinstance(environment, TaskEnvironment)
    assert not (tmp_path / "uv.lock").exists()
