from lutra import LutraServiceClient, PingRequest
from lutra._gen.lutra.v1.lutra_connect import LutraServiceClient as GeneratedClient  # ruff: ignore[import-private-name]


def test_client_reexport_is_generated_client() -> None:
    assert LutraServiceClient is GeneratedClient
    assert PingRequest(message="hi").message == "hi"
