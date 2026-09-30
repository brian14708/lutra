"""Create and read zstd:chunked tar archives."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import shutil
import stat
import struct
import tarfile
import tempfile
from operator import itemgetter
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, cast

import zstandard

if TYPE_CHECKING:
    from collections.abc import Iterator


_SKIP_MAGIC = 0x184D2A50
_FOOTER_MAGIC = 0x78556E496C554E47
_FOOTER = struct.Struct("<II8Q")
_SKIP_HEADER = struct.Struct("<II")
_MAX_METADATA = 64 << 20
_MAX_SKIP_SIZE = (1 << 32) - 1
_REGULAR_MODE = 0o644
_EXECUTABLE_MODE = 0o755
_DIRECTORY_MODE = 0o755
_ZSTD_LEVEL = 3


class ArchiveError(ValueError):
    """An archive is malformed or contains unsafe paths."""


class _LimitedReader:
    def __init__(self, source: BinaryIO, length: int) -> None:
        self.source = source
        self.remaining = length

    def read(self, size: int = -1) -> bytes:
        if self.remaining == 0:
            return b""
        amount = self.remaining if size < 0 else min(size, self.remaining)
        data = self.source.read(amount)
        self.remaining -= len(data)
        return data


def _members(source: Path) -> Iterator[tuple[Path, str]]:
    if source.is_symlink():
        msg = "archive source must not be a symlink"
        raise ArchiveError(msg)
    if source.is_file():
        yield source, source.name
    elif source.is_dir():
        members = ((path, path.relative_to(source).as_posix()) for path in source.rglob("*"))
        yield from sorted(members, key=itemgetter(1))
    else:
        msg = "archive source must be a file or directory"
        raise ArchiveError(msg)


def _tar_info(name: str, mode: int) -> tarfile.TarInfo:
    entry = tarfile.TarInfo(name)
    entry.uid = 0
    entry.gid = 0
    entry.uname = ""
    entry.gname = ""
    entry.mtime = 0
    entry.mode = mode
    entry.pax_headers = {}
    return entry


def _write_tar(source: Path, target: BinaryIO) -> list[tuple[tarfile.TarInfo, int]]:
    members: list[tuple[tarfile.TarInfo, int]] = []
    with tarfile.open(fileobj=target, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for path, name in _members(source):
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                entry = _tar_info(name + "/", _DIRECTORY_MODE)
                entry.type = tarfile.DIRTYPE
                archive.addfile(entry)
            elif stat.S_ISREG(info.st_mode):
                mode = _EXECUTABLE_MODE if info.st_mode & 0o111 else _REGULAR_MODE
                entry = _tar_info(name, mode)
                entry.type = tarfile.REGTYPE
                entry.size = info.st_size
                with path.open("rb") as contents:
                    archive.addfile(entry, contents)
            else:
                msg = f"unsupported archive entry: {name}"
                raise ArchiveError(msg)
            padded_size = (entry.size + tarfile.BLOCKSIZE - 1) // tarfile.BLOCKSIZE
            members.append((entry, target.tell() - padded_size * tarfile.BLOCKSIZE))
    return members


def _frame(source: BinaryIO, output: BinaryIO, length: int) -> str:
    digest = hashlib.sha256()
    with zstandard.ZstdCompressor(level=_ZSTD_LEVEL).stream_writer(output, closefd=False) as writer:
        remaining = length
        while remaining:
            chunk = source.read(min(1 << 20, remaining))
            if not chunk:
                msg = "tar data ended unexpectedly"
                raise ArchiveError(msg)
            writer.write(chunk)
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def _segment(
    source: BinaryIO, output: BinaryIO, length: int, entries: list[dict[str, object]]
) -> None:
    if length == 0:
        return
    data = source.read(length)
    if len(data) != length:
        msg = "tar data ended unexpectedly"
        raise ArchiveError(msg)
    output.write(zstandard.ZstdCompressor(level=_ZSTD_LEVEL).compress(data))
    entries.append({
        "type": 2,
        "payload": base64.b64encode(data).decode("ascii"),
        "position": len(entries),
    })


def _metadata(output: BinaryIO, data: bytes) -> tuple[int, int, int]:
    if len(data) > _MAX_METADATA:
        msg = "archive metadata is too large"
        raise ArchiveError(msg)
    compressed = zstandard.ZstdCompressor(level=_ZSTD_LEVEL).compress(data)
    if len(compressed) > _MAX_SKIP_SIZE:
        msg = "archive metadata is too large"
        raise ArchiveError(msg)
    output.write(struct.pack("<II", _SKIP_MAGIC, len(compressed)))
    offset = output.tell()
    output.write(compressed)
    return offset, len(compressed), len(data)


def create_archive(source: Path, output: Path) -> None:
    """Write a sorted zstd:chunked tar archive with normalized tar metadata."""
    source = Path(source)
    output = Path(output)
    with tempfile.TemporaryFile() as raw:
        members = _write_tar(source, raw)
        raw.seek(0, os.SEEK_END)
        tar_length = raw.tell()
        raw.seek(0)
        manifest: list[dict[str, object]] = []
        tarsplit: list[dict[str, object]] = []
        with output.open("wb") as compressed:
            cursor = 0
            for member, offset_data in members:
                _segment(raw, compressed, offset_data - cursor, tarsplit)
                cursor = offset_data
                item: dict[str, object] = {
                    "type": "dir" if member.isdir() else "reg",
                    "name": member.name,
                    "mode": member.mode,
                    "modtime": "1970-01-01T00:00:00Z",
                }
                if member.isfile():
                    item["size"] = member.size
                    if member.size:
                        start = compressed.tell()
                        digest = _frame(raw, compressed, member.size)
                        item.update(
                            digest=f"sha256:{digest}", offset=start, endOffset=compressed.tell()
                        )
                        tarsplit.append({
                            "type": 1,
                            "name": member.name,
                            "size": member.size,
                            "position": len(tarsplit),
                        })
                    cursor += member.size
                manifest.append(item)
            _segment(raw, compressed, tar_length - cursor, tarsplit)
            manifest_bytes = json.dumps(
                {"version": 1, "entries": manifest}, separators=(",", ":")
            ).encode()
            split_bytes = b"".join(
                json.dumps(entry, separators=(",", ":")).encode() + b"\n" for entry in tarsplit
            )
            manifest_location = _metadata(compressed, manifest_bytes)
            split_location = _metadata(compressed, split_bytes)
            compressed.write(
                _FOOTER.pack(_SKIP_MAGIC, 64, *manifest_location, 1, *split_location, _FOOTER_MAGIC)
            )


def _safe_name(name: str) -> tuple[str, ...]:
    parts = tuple(name.rstrip("/").split("/"))
    if (
        not name
        or name.startswith("/")
        or "\\" in name
        or any(part in {"", ".", ".."} for part in parts)
    ):
        msg = f"unsafe archive path: {name!r}"
        raise ArchiveError(msg)
    return parts


def _read_metadata(source: BinaryIO, offset: int, compressed: int, size: int, end: int) -> bytes:
    if (
        size > _MAX_METADATA
        or compressed > _MAX_METADATA
        or offset < _SKIP_HEADER.size
        or offset + compressed > end
    ):
        msg = "invalid archive metadata range"
        raise ArchiveError(msg)
    source.seek(offset - _SKIP_HEADER.size)
    if source.read(_SKIP_HEADER.size) != _SKIP_HEADER.pack(_SKIP_MAGIC, compressed):
        msg = "invalid archive metadata frame"
        raise ArchiveError(msg)
    try:
        data = zstandard.ZstdDecompressor().decompress(
            source.read(compressed), max_output_size=size
        )
    except zstandard.ZstdError as error:
        msg = "invalid compressed archive metadata"
        raise ArchiveError(msg) from error
    if len(data) != size:
        msg = "archive metadata size mismatch"
        raise ArchiveError(msg)
    return data


def _footer_locations(source: BinaryIO) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
    source.seek(0, os.SEEK_END)
    end = source.tell()
    if end < _FOOTER.size:
        msg = "missing zstd:chunked footer"
        raise ArchiveError(msg)
    source.seek(end - _FOOTER.size)
    fields = _FOOTER.unpack(source.read(_FOOTER.size))
    if (fields[0], fields[1], fields[5], fields[9]) != (_SKIP_MAGIC, 64, 1, _FOOTER_MAGIC):
        msg = "invalid zstd:chunked footer"
        raise ArchiveError(msg)
    return fields[2:5], fields[6:9]


def _manifest(source: BinaryIO) -> list[dict[str, object]]:
    manifest_location, split_location = _footer_locations(source)
    source.seek(0, os.SEEK_END)
    end = source.tell() - _FOOTER.size
    manifest_data = _read_metadata(source, *manifest_location, end)
    _read_metadata(source, *split_location, end)
    try:
        document = json.loads(manifest_data)
    except (TypeError, ValueError) as error:
        msg = "invalid archive manifest"
        raise ArchiveError(msg) from error
    if not isinstance(document, dict) or document.get("version") != 1:
        msg = "invalid archive manifest"
        raise ArchiveError(msg)
    entries = document.get("entries")
    if not isinstance(entries, list):
        msg = "invalid archive manifest"
        raise ArchiveError(msg)
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            msg = "invalid archive entry"
            raise ArchiveError(msg)
        name = entry.get("name")
        if not isinstance(name, str) or name in seen or entry.get("type") not in {"dir", "reg"}:
            msg = "invalid archive entry"
            raise ArchiveError(msg)
        _safe_name(name)
        seen.add(name)
    return entries


def _stream_content(reader: BinaryIO, output: BinaryIO, digest: hashlib._Hash, size: int) -> int:
    copied = 0
    while chunk := reader.read(1 << 20):
        copied += len(chunk)
        if copied > size:
            break
        digest.update(chunk)
        output.write(chunk)
    return copied


def _copy_content(source: BinaryIO, entry: dict[str, object], limit: int, output: BinaryIO) -> None:
    size = entry.get("size", 0)
    if not isinstance(size, int) or size < 0:
        msg = "invalid archive file size"
        raise ArchiveError(msg)
    if size == 0:
        return
    start, end = entry.get("offset"), entry.get("endOffset")
    if (
        not isinstance(start, int)
        or not isinstance(end, int)
        or start < 0
        or end <= start
        or end > limit
    ):
        msg = "invalid archive file range"
        raise ArchiveError(msg)
    source.seek(start)
    digest = hashlib.sha256()
    try:
        bounded = _LimitedReader(source, end - start)
        with zstandard.ZstdDecompressor().stream_reader(
            cast("BinaryIO", bounded), read_across_frames=True
        ) as reader:
            copied = _stream_content(reader, output, digest, size)
    except zstandard.ZstdError as error:
        msg = "invalid compressed archive file"
        raise ArchiveError(msg) from error
    if (
        copied != size
        or bounded.remaining != 0
        or entry.get("digest") != f"sha256:{digest.hexdigest()}"
    ):
        msg = "archive file checksum mismatch"
        raise ArchiveError(msg)


def read_archive(archive: Path, name: str) -> bytes:
    """Read one regular file by name from a zstd:chunked archive.

    Returns:
        The file contents.

    Raises:
        KeyError: If the file is absent.

    """
    _safe_name(name)
    with Path(archive).open("rb") as source:
        entries = _manifest(source)
        source.seek(0, os.SEEK_END)
        limit = source.tell() - _FOOTER.size
        for entry in entries:
            if entry["name"] == name and entry["type"] == "reg":
                output = io.BytesIO()
                _copy_content(source, entry, limit, output)
                return output.getvalue()
    raise KeyError(name)


def _extract_member(tar: tarfile.TarFile, member: tarfile.TarInfo, root: Path) -> None:
    target = root.joinpath(*_safe_name(member.name))
    if not target.parent.resolve().is_relative_to(root) or target.is_symlink():
        msg = "archive path escapes destination"
        raise ArchiveError(msg)
    if member.isdir():
        target.mkdir(parents=True, exist_ok=True)
    elif member.isfile():
        contents = tar.extractfile(member)
        if contents is None:
            msg = "invalid archive file"
            raise ArchiveError(msg)
        target.parent.mkdir(parents=True, exist_ok=True)
        with contents, tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as output:
            temporary = Path(output.name)
            try:
                shutil.copyfileobj(contents, output)
            except Exception:
                temporary.unlink(missing_ok=True)
                raise
        temporary.replace(target)
    else:
        msg = f"unsupported archive entry: {member.name}"
        raise ArchiveError(msg)


def extract_archive(archive: Path, destination: Path) -> None:
    """Extract all regular files and directories from the tar stream.

    Raises:
        ArchiveError: If the archive is malformed or contains an unsafe path.

    """
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    root = destination.resolve()
    try:
        with (
            Path(archive).open("rb") as source,
            zstandard.ZstdDecompressor().stream_reader(source, read_across_frames=True) as reader,
            tarfile.open(fileobj=reader, mode="r|") as tar,
        ):
            for member in tar:
                _extract_member(tar, member, root)
    except (tarfile.TarError, zstandard.ZstdError) as error:
        msg = "invalid compressed tar archive"
        raise ArchiveError(msg) from error
