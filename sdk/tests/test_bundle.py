from __future__ import annotations

import importlib
import io
import textwrap
import zipfile
from typing import TYPE_CHECKING, cast

from lutra import client
from lutra.task import TaskEnvironment

if TYPE_CHECKING:
    from pathlib import Path

    import pytest
    from lutra.task import Task


def test_bundle_includes_local_package_helpers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = tmp_path / "bundle_case"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "tasks.py").write_text(
        textwrap.dedent(
            """
            from bundle_case.helper import helper

            def task(value):
                return helper(value)
            """
        )
    )
    (package / "helper.py").write_text("def helper(value):\n    return value\n")
    monkeypatch.syspath_prepend(str(tmp_path))

    module = importlib.import_module("bundle_case.tasks")
    environment = TaskEnvironment("bundle-case")
    task = cast("Task", environment.task(module.task))

    with zipfile.ZipFile(io.BytesIO(client._bundle(task))) as archive:  # ruff: ignore[private-member-access]
        names = set(archive.namelist())
    assert {"bundle_case/__init__.py", "bundle_case/tasks.py", "bundle_case/helper.py"} <= names
