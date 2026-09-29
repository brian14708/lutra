import asyncio
import base64
import hashlib

import cbor2
import pytest
from lutra.value import (
    BlobRef,
    UnsupportedCborValueError,
    ValueCodecError,
    dumps,
    loads,
    value_hash,
)


@pytest.mark.parametrize(
    ("value", "encoded", "digest"),
    [
        (None, "f6", "b0b2988b6bbe724bacda5e9e524736de0bc7dae41c46b4213c50e1d35d4e5f13"),
        (True, "f5", "27abdeddfe8503496adeb623466caa47da5f63abd2bc6fa19f6cfcb73ecfed70"),
        (0, "00", "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d"),
        (-1, "20", "36a9e7f1c95b82ffb99743e0c5c4ce95d83c9a430aac59f84ef3cbfab6145068"),
        (24, "1818", "7c6348f7ea8a4e831aafe688a2557d7a3ab0be8549cdb190a029956ade971b15"),
        (1.5, "f93e00", "b68bb45ecab0329ab815daf44f5a02d2a11a8ab87fbbdf4b08bcae00cada0324"),
        (-0.0, "f98000", "e95873bb6549017f8cd72cc8af178667d65261077ca89caf09a330742965f851"),
        (
            2**64,
            "c249010000000000000000",
            "140308c2b6fefc2dab159b96a16190a016ac4a30f6130591e33a01b741a6538a",
        ),
        (
            -(2**64) - 1,
            "c349010000000000000000",
            "ea7a19842e8e0bb0bb6f8b6c4d9e9a762e99d2bd9c961aa294da66913fc99615",
        ),
        (
            2**128,
            "c2510100000000000000000000000000000000",
            "926c44ec60a6e7187b2f586a67c762e70b63f9cdaa33086e2d36c009fd039f9a",
        ),
        (
            {"aa": 1, "b": 2},
            "a261620262616101",
            "9ba01bc6133f5b08a6b955cc1372f5556170bc093939214953d0f05eb1e7630f",
        ),
        (
            {"z": 2, "a": [b"hi", False]},
            "a2616182426869f4617a02",
            "b1aeec533bf3a77ff65021fd8b227488d3f67ea6ac53b7b69c3803a14e01011c",
        ),
    ],
)
def test_shared_vectors(value: object, encoded: str, digest: str) -> None:
    assert dumps(value).hex() == encoded
    assert value_hash(value).hex() == digest


@pytest.mark.asyncio
async def test_blob_resolution() -> None:
    digest = hashlib.sha256(b"hello").digest()
    name = base64.b64encode(digest).decode("ascii")
    ref = BlobRef(f"blob:application/cbor,{name}", resolve=True, mime_type="application/cbor")

    async def fetch(uri: str) -> bytes:
        await asyncio.sleep(0)
        assert uri == ref.uri
        return dumps({"answer": 42})

    assert await loads(dumps(ref), fetch) == {"answer": 42}
    assert await loads(dumps(BlobRef(ref.uri))) == BlobRef(ref.uri)
    with pytest.raises(ValueError, match="invalid blob digest name"):
        BlobRef(f"blob:input:{name}")
    with pytest.raises(ValueError, match="resolver required"):
        await loads(dumps(ref))


@pytest.mark.asyncio
async def test_nested_blob_refs_use_cbor_hooks() -> None:
    name = base64.b64encode(hashlib.sha256(b"nested").digest()).decode("ascii")
    ref = BlobRef(f"blob:application/octet-stream,{name}")
    value = {"items": [ref, {"ref": ref}]}
    tag = cbor2.CBORTag(32, ref.uri_string)
    assert dumps(value) == cbor2.dumps({"items": [tag, {"ref": tag}]}, canonical=True)
    assert await loads(dumps(value)) == value


@pytest.mark.asyncio
async def test_data_uri_resolution() -> None:
    ref = BlobRef("data:text/plain;base64,SGVsbG8=", resolve=True, mime_type="text/plain")
    assert cbor2.loads(dumps(ref)) == cbor2.CBORTag(32, "data:text/plain;base64;resolve,SGVsbG8=")
    assert await loads(dumps({"items": [ref]})) == {"items": [b"Hello"]}
    legacy = cbor2.dumps(cbor2.CBORTag(32, "data:text/plain;base64;resolve=true,SGVsbG8="))
    assert await loads(legacy) == b"Hello"


@pytest.mark.parametrize("value", [{1: "value"}, cbor2.CBORTag(40, 1)])
def test_dumps_accepts_cbor2_values(value: object) -> None:
    assert dumps(value) == cbor2.dumps(value, canonical=True)


def test_dumps_rejects_unknown_python_type() -> None:
    with pytest.raises(UnsupportedCborValueError):
        dumps({"nested": [object()]})


@pytest.mark.asyncio
async def test_loads_rejects_unsupported_tags() -> None:
    with pytest.raises(ValueCodecError, match="unsupported CBOR tag"):
        await loads(cbor2.dumps({"nested": [cbor2.CBORTag(40, 1)]}))
