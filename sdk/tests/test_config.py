"""Configuration validation and task import behavior."""

import base64
from pathlib import Path
from typing import TYPE_CHECKING, cast

import cbor2
import pytest
from lutra import ConfigError
from lutra._gen.lutra.task.v1.task_pb import ExecuteRequest, ExecuteResponse
from lutra.serve import TaskAPIClient
from lutra.serve.__main__ import _bundled_handler, _load_config
from lutra.serve._host import _Host, _TaskService
from lutra.value import dumps

if TYPE_CHECKING:
    from connectrpc.request import RequestContext


@pytest.mark.asyncio
async def test_config_import_context(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "task.py"
    source.write_text(
        "import os\n"
        "from lutra import TaskEnvironment, ConfigBinding, current_context\n"
        "IMPORTED = [os.environ['TOKEN'], current_context().config['value']]\n"
        "env = TaskEnvironment('test')\n"
        "@env.task(config=(ConfigBinding('value', 'service/value'),))\n"
        "def work():\n"
        "    return [IMPORTED, current_context().config['value']]\n"
    )
    monkeypatch.setenv("LUTRA_BUNDLE_ROOT", str(tmp_path))
    monkeypatch.setenv("TOKEN", "provider-value")
    monkeypatch.setenv("LUTRA_ENVIRONMENTS_JSON", "[]")
    monkeypatch.setenv(
        "LUTRA_CONFIG_CBOR",
        base64.b64encode(
            dumps({"value": {"present": True, "value": None, "required": True, "sensitive": False}})
        ).decode(),
    )
    handler, retry = _bundled_handler("file:task.py:work")
    service = _TaskService(handler, retry)
    service.api_client = TaskAPIClient(cast("_Host", object()))
    ctx = cast("RequestContext[ExecuteRequest, ExecuteResponse]", None)
    response = await service.execute(
        ExecuteRequest(
            invocation_id="test",
            run_id="run",
            action_id="test",
            content_type="application/cbor",
            input=dumps([[], {}]),
        ),
        ctx,
    )
    assert response.result_cbor == dumps([["provider-value", None], None])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [
        None,
        {"value": {}},
        {"value": {"present": True, "value": 1, "required": True, "sensitive": 1}},
    ],
)
async def test_invalid_config(value: object, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LUTRA_CONFIG_CBOR", base64.b64encode(dumps(value)).decode())
    with pytest.raises(ConfigError, match=r"config\.invalid"):
        await _load_config()


@pytest.mark.asyncio
async def test_missing_config_prevents_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "task.py"
    source.write_text("raise AssertionError('task imported')\n")
    monkeypatch.setenv("LUTRA_BUNDLE_ROOT", str(tmp_path))
    monkeypatch.setenv("LUTRA_ENVIRONMENTS_JSON", "[]")
    monkeypatch.setenv(
        "LUTRA_CONFIG_CBOR",
        base64.b64encode(
            dumps({
                "value": {"present": False, "value": None, "required": True, "sensitive": False}
            })
        ).decode(),
    )
    handler, retry = _bundled_handler("file:task.py:work")
    service = _TaskService(handler, retry)
    service.api_client = TaskAPIClient(cast("_Host", object()))
    ctx = cast("RequestContext[ExecuteRequest, ExecuteResponse]", None)
    response = await service.execute(
        ExecuteRequest(invocation_id="test", run_id="run", action_id="test"), ctx
    )
    assert cbor2.loads(response.result_cbor).value["message"] == "config.missing"
    assert cbor2.loads(response.result_cbor).value["cacheable"] is False
    assert cbor2.loads(response.result_cbor).value["details"] is None
