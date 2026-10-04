"""Create and read zstd:chunked tar archives."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import posixpath
import re
import shutil
import stat
import struct
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import datetime
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
_MAX_ARCHIVE_SIZE = 2 << 30
_MAX_ARCHIVE_ENTRIES = 100_000


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


def _write_tar(
    source: Path, target: BinaryIO, *, prefix: str = "", allow_symlinks: bool = True
) -> list[tuple[tarfile.TarInfo, int]]:
    members: list[tuple[tarfile.TarInfo, int]] = []
    with tarfile.open(fileobj=target, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for path, name in _members(source):
            arcname = f"{prefix}/{name}" if prefix else name
            _safe_name(arcname)
            info = path.lstat()
            size = info.st_size if stat.S_ISREG(info.st_mode) else 0
            if (
                target.tell() + size + 1024 > _MAX_ARCHIVE_SIZE
                or len(members) >= _MAX_ARCHIVE_ENTRIES
            ):
                msg = "archive exceeds size or entry limit"
                raise ArchiveError(msg)
            if stat.S_ISDIR(info.st_mode):
                entry = _tar_info(arcname + "/", _DIRECTORY_MODE)
                entry.type = tarfile.DIRTYPE
                archive.addfile(entry)
            elif stat.S_ISREG(info.st_mode):
                mode = _EXECUTABLE_MODE if info.st_mode & 0o111 else _REGULAR_MODE
                entry = _tar_info(arcname, mode)
                entry.type = tarfile.REGTYPE
                entry.size = info.st_size
                with path.open("rb") as contents:
                    archive.addfile(entry, contents)
            elif stat.S_ISLNK(info.st_mode):
                if not allow_symlinks:
                    msg = f"symlinks are not allowed in snapshots: {arcname}"
                    raise ArchiveError(msg)
                entry = _tar_info(arcname, _REGULAR_MODE)
                entry.type = tarfile.SYMTYPE
                entry.linkname = str(path.readlink())
                archive.addfile(entry)
            else:
                msg = f"unsupported archive entry: {arcname}"
                raise ArchiveError(msg)
            padded_size = (entry.size + tarfile.BLOCKSIZE - 1) // tarfile.BLOCKSIZE
            members.append((entry, target.tell() - padded_size * tarfile.BLOCKSIZE))
    return members


def _frame(
    source: BinaryIO,
    output: BinaryIO,
    length: int,
    compressor: zstandard.ZstdCompressor,
    digest: hashlib._Hash | None = None,
) -> None:
    with compressor.stream_writer(output, closefd=False) as writer:
        remaining = length
        while remaining:
            chunk = source.read(min(1 << 20, remaining))
            if not chunk:
                msg = "tar data ended unexpectedly"
                raise ArchiveError(msg)
            writer.write(chunk)
            if digest is not None:
                digest.update(chunk)
            remaining -= len(chunk)


def _metadata(
    output: BinaryIO, data: bytes, compressor: zstandard.ZstdCompressor
) -> tuple[int, int, int]:
    if len(data) > _MAX_METADATA:
        msg = "archive metadata is too large"
        raise ArchiveError(msg)
    compressed = compressor.compress(data)
    if len(compressed) > _MAX_SKIP_SIZE:
        msg = "archive metadata is too large"
        raise ArchiveError(msg)
    output.write(struct.pack("<II", _SKIP_MAGIC, len(compressed)))
    offset = output.tell()
    output.write(compressed)
    return offset, len(compressed), len(data)


def _write_archive(
    raw: BinaryIO, output: BinaryIO, members: list[tuple[tarfile.TarInfo, int]], tar_length: int
) -> None:
    compressor = zstandard.ZstdCompressor(level=_ZSTD_LEVEL)
    manifest: list[dict[str, object]] = []
    cursor = 0
    for member, offset_data in members:
        if offset_data > cursor:
            _frame(raw, output, offset_data - cursor, compressor)
        cursor = offset_data
        item: dict[str, object] = {
            "type": "dir" if member.isdir() else "symlink" if member.issym() else "reg",
            "name": member.name,
            "mode": member.mode,
            "modtime": "1970-01-01T00:00:00Z",
        }
        if member.issym():
            item["linkName"] = member.linkname
        if member.isfile() and member.size:
            digest = hashlib.sha256()
            start = output.tell()
            _frame(raw, output, member.size, compressor, digest)
            item.update(
                size=member.size,
                digest=f"sha256:{digest.hexdigest()}",
                offset=start,
                endOffset=output.tell(),
            )
            cursor += member.size
        manifest.append(item)
    _frame(raw, output, tar_length - cursor, compressor)
    manifest_bytes = json.dumps({"version": 1, "entries": manifest}, separators=(",", ":")).encode()
    manifest_location = _metadata(output, manifest_bytes, compressor)
    output.write(_FOOTER.pack(_SKIP_MAGIC, 64, *manifest_location, 1, 0, 0, 0, _FOOTER_MAGIC))
    if output.tell() > _MAX_ARCHIVE_SIZE:
        msg = "archive exceeds size limit"
        raise ArchiveError(msg)


def create_archive(
    source: Path, output: Path, *, prefix: str = "", allow_symlinks: bool = True
) -> None:
    """Write a sorted zstd:chunked tar archive with normalized tar metadata.

    Raises:
        ArchiveError: If the source is unsafe, unsupported, or exceeds archive limits.

    """
    source = Path(source)
    output = Path(output)
    if prefix:
        _safe_name(prefix)
    with tempfile.TemporaryFile() as raw:
        members = _write_tar(source, raw, prefix=prefix, allow_symlinks=allow_symlinks)
        tar_length = raw.tell()
        if tar_length > _MAX_ARCHIVE_SIZE:
            msg = "archive exceeds size or entry limit"
            raise ArchiveError(msg)
        raw.seek(0)
        with tempfile.NamedTemporaryFile(
            dir=output.parent, prefix=f".{output.name}.", delete=False
        ) as compressed:
            temporary = Path(compressed.name)
            try:
                _write_archive(raw, cast("BinaryIO", compressed), members, tar_length)
                compressed.close()
                temporary.replace(output)
            except BaseException:
                temporary.unlink(missing_ok=True)
                raise


def _safe_name(name: str) -> tuple[str, ...]:
    parts = tuple(name.removesuffix("/").split("/"))
    if (
        not name
        or name.startswith("/")
        or "\\" in name
        or "\x00" in name
        or any(part in {"", ".", ".."} for part in parts)
    ):
        msg = f"unsafe archive path: {name!r}"
        raise ArchiveError(msg)
    return parts


def _decompress_frame(source: BinaryIO, length: int, size: int, output: BinaryIO) -> str:
    bounded = _LimitedReader(source, length)
    decoder = zstandard.ZstdDecompressor(max_window_size=_MAX_ARCHIVE_SIZE >> 10).decompressobj()
    digest = hashlib.sha256()
    copied = 0
    while chunk := bounded.read(16 << 10):
        try:
            data = decoder.decompress(chunk)
        except zstandard.ZstdError as error:
            msg = "invalid compressed archive frame"
            raise ArchiveError(msg) from error
        copied += len(data)
        if copied > size:
            msg = "archive decompressed size mismatch"
            raise ArchiveError(msg)
        output.write(data)
        digest.update(data)
        if decoder.eof:
            break
    if copied != size or not decoder.eof or decoder.unused_data or bounded.remaining:
        msg = "archive frame size or completion mismatch"
        raise ArchiveError(msg)
    return f"sha256:{digest.hexdigest()}"


def _read_metadata(source: BinaryIO, offset: int, compressed: int, size: int, end: int) -> bytes:
    if (
        not 0 < size <= _MAX_METADATA
        or not 0 < compressed <= _MAX_METADATA
        or offset < _SKIP_HEADER.size
        or offset + compressed != end
    ):
        msg = "invalid archive metadata range"
        raise ArchiveError(msg)
    source.seek(offset - _SKIP_HEADER.size)
    if source.read(_SKIP_HEADER.size) != _SKIP_HEADER.pack(_SKIP_MAGIC, compressed):
        msg = "invalid archive metadata frame"
        raise ArchiveError(msg)
    output = io.BytesIO()
    _decompress_frame(source, compressed, size, output)
    return output.getvalue()


def _footer_locations(source: BinaryIO) -> tuple[tuple[int, int, int], int]:
    source.seek(0, os.SEEK_END)
    end = source.tell()
    if not _FOOTER.size <= end <= _MAX_ARCHIVE_SIZE:
        msg = "missing footer or archive exceeds size limit"
        raise ArchiveError(msg)
    source.seek(end - _FOOTER.size)
    fields = _FOOTER.unpack(source.read(_FOOTER.size))
    if (fields[0], fields[1], fields[5], fields[9]) != (_SKIP_MAGIC, 64, 1, _FOOTER_MAGIC):
        msg = "invalid zstd:chunked footer"
        raise ArchiveError(msg)
    if fields[6:9] != (0, 0, 0):
        msg = "tar-split data is not supported"
        raise ArchiveError(msg)
    return fields[2:5], end - _FOOTER.size


@dataclass(frozen=True)
class _Entry:
    name: str
    kind: str
    size: int
    start: int
    end: int
    digest: str


def _integer(entry: dict[str, object], field: str, maximum: int = _MAX_ARCHIVE_SIZE) -> int:
    value = entry.get(field, 0)
    if type(value) is not int or not 0 <= value <= maximum:
        msg = f"invalid archive entry {field}"
        raise ArchiveError(msg)
    return value


def _string(entry: dict[str, object], field: str) -> str:
    value = entry.get(field, "")
    if not isinstance(value, str):
        msg = f"invalid archive entry {field}"
        raise ArchiveError(msg)
    return value


def _validate_link(name: str, link: str) -> None:
    if not link or "\\x00" in link or "\\\\" in link:
        msg = "invalid archive symlink target"
        raise ArchiveError(msg)
    if link.startswith("/"):
        resolved = Path(posixpath.normpath(link))
        if not any(resolved.is_relative_to(root) for root in ("/nix/store", "/usr")):
            msg = "archive symlink escapes allowed runtime roots"
            raise ArchiveError(msg)
    else:
        resolved_name = posixpath.normpath(posixpath.join(posixpath.dirname(name), link))
        if resolved_name == ".." or resolved_name.startswith("../"):
            msg = "archive symlink escapes destination"
            raise ArchiveError(msg)


def _entry_metadata(entry: dict[str, object]) -> None:
    _integer(entry, "mode", 0o7777)
    _integer(entry, "uid", (1 << 32) - 1)
    _integer(entry, "gid", (1 << 32) - 1)
    modtime = _string(entry, "modtime")
    if modtime:
        try:
            parsed = datetime.fromisoformat(modtime)
        except ValueError as error:
            msg = "invalid archive entry modtime"
            raise ArchiveError(msg) from error
        if (
            not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", modtime
            )
            or parsed.tzinfo is None
        ):
            msg = "invalid archive entry modtime"
            raise ArchiveError(msg)


def _parse_entry(value: object, limit: int) -> _Entry:
    if not isinstance(value, dict):
        msg = "invalid archive entry"
        raise ArchiveError(msg)
    name, kind = _string(value, "name"), _string(value, "type")
    _safe_name(name)
    if kind not in {"reg", "dir", "symlink"} or (kind != "dir" and name.endswith("/")):
        msg = "unsupported archive entry type or path"
        raise ArchiveError(msg)
    _entry_metadata(value)
    size = _integer(value, "size")
    start, end = _integer(value, "offset"), _integer(value, "endOffset")
    digest, link = _string(value, "digest"), _string(value, "linkName")
    if kind == "reg" and size:
        if end <= start or end > limit:
            msg = "invalid archive file range"
            raise ArchiveError(msg)
        if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
            msg = "invalid archive file digest"
            raise ArchiveError(msg)
    elif size or start or end or digest:
        msg = "invalid empty or non-regular archive entry"
        raise ArchiveError(msg)
    if kind == "symlink":
        _validate_link(name, link)
    elif link:
        msg = "invalid archive entry linkName"
        raise ArchiveError(msg)
    return _Entry(name, kind, size, start, end, digest)


def _manifest(source: BinaryIO) -> dict[str, _Entry]:
    location, end = _footer_locations(source)
    data = _read_metadata(source, *location, end)
    try:
        document = json.loads(data)
    except (ValueError, RecursionError) as error:
        msg = "invalid archive manifest"
        raise ArchiveError(msg) from error
    if (
        not isinstance(document, dict)
        or type(document.get("version")) is not int
        or document["version"] != 1
    ):
        msg = "invalid archive manifest"
        raise ArchiveError(msg)
    values = document.get("entries")
    if not isinstance(values, list) or len(values) > _MAX_ARCHIVE_ENTRIES:
        msg = "invalid archive entries or entry limit exceeded"
        raise ArchiveError(msg)
    entries: dict[str, _Entry] = {}
    ranges: list[tuple[int, int]] = []
    total_size = 0
    for value in values:
        entry = _parse_entry(value, location[0] - _SKIP_HEADER.size)
        key = "/".join(_safe_name(entry.name))
        if key in entries:
            msg = "duplicate archive path"
            raise ArchiveError(msg)
        entries[key] = entry
        total_size += entry.size
        if total_size > _MAX_ARCHIVE_SIZE:
            msg = "archive exceeds size limit"
            raise ArchiveError(msg)
        if entry.size:
            ranges.append((entry.start, entry.end))
    previous_end = 0
    for start, finish in sorted(ranges):
        if start < previous_end:
            msg = "overlapping archive file ranges"
            raise ArchiveError(msg)
        previous_end = finish
    return entries


def zstd_chunked_manifest_metadata(archive: Path) -> dict[str, str]:
    """Return standard zstd:chunked annotations for an archive manifest.

    Returns:
        Manifest checksum and position annotations.

    """
    with Path(archive).open("rb") as source:
        location, end = _footer_locations(source)
        data = _read_metadata(source, *location, end)
    offset, compressed, size = location
    return {
        "zstd-chunked-manifest-checksum": (f"sha256:{hashlib.sha256(data).hexdigest()}"),
        "zstd-chunked-manifest-position": f"{offset}:{compressed}:{size}:1",
    }


def read_archive(archive: Path, name: str) -> bytes:
    """Read one regular file by name from a zstd:chunked archive.

    Returns:
        The file contents.

    Raises:
        KeyError: If the file is absent.
        ArchiveError: If the archive is malformed or the file fails verification.

    """
    _safe_name(name)
    with Path(archive).open("rb") as source:
        entry = _manifest(source).get(name)
        if entry is None or entry.kind != "reg":
            raise KeyError(name)
        output = io.BytesIO()
        if entry.size:
            source.seek(entry.start)
            digest = _decompress_frame(source, entry.end - entry.start, entry.size, output)
            if digest != entry.digest:
                msg = "archive file checksum mismatch"
                raise ArchiveError(msg)
        return output.getvalue()


def _extract_symlink(target: Path, link: str, root: Path) -> None:
    if Path(link).is_absolute():
        resolved = Path(os.path.normpath(link))
        if not any(resolved.is_relative_to(prefix) for prefix in ("/nix/store", "/usr")):
            msg = "archive symlink escapes allowed runtime roots"
            raise ArchiveError(msg)
    elif not target.parent.joinpath(link).resolve().is_relative_to(root):
        msg = "archive symlink escapes destination"
        raise ArchiveError(msg)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.symlink_to(link)


def _extract_member(tar: tarfile.TarFile, member: tarfile.TarInfo, root: Path) -> None:
    target = root.joinpath(*_safe_name(member.name))
    if not target.parent.resolve().is_relative_to(root) or target.is_symlink():
        msg = "archive path escapes destination"
        raise ArchiveError(msg)
    if member.isdir():
        target.mkdir(parents=True, exist_ok=True)
    elif member.issym():
        _extract_symlink(target, member.linkname, root)
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
                if member.mode & 0o100:
                    temporary.chmod(0o700)
            except Exception:
                temporary.unlink(missing_ok=True)
                raise
        temporary.replace(target)
    else:
        msg = f"unsupported archive entry: {member.name}"
        raise ArchiveError(msg)


def extract_archive(archive: Path, destination: Path, *, allow_symlinks: bool = True) -> None:
    """Extract files, directories, and symlinks from the tar stream.

    Raises:
        ArchiveError: If the archive is malformed or contains an unsafe path.

    """
    if Path(archive).stat().st_size > _MAX_ARCHIVE_SIZE:
        msg = "archive exceeds size limit"
        raise ArchiveError(msg)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    root = destination.resolve()
    try:  # ruff: ignore[too-many-statements-in-try-clause]
        with (
            Path(archive).open("rb") as source,
            zstandard.ZstdDecompressor().stream_reader(source, read_across_frames=True) as reader,
            tarfile.open(fileobj=reader, mode="r|") as tar,
        ):
            size = 0
            for count, member in enumerate(tar, 1):
                size += member.size
                if size > _MAX_ARCHIVE_SIZE or count > _MAX_ARCHIVE_ENTRIES:
                    msg = "archive exceeds size or entry limit"
                    raise ArchiveError(msg)
                if member.issym() and not allow_symlinks:
                    msg = f"symlinks are not allowed in snapshots: {member.name}"
                    raise ArchiveError(msg)
                _extract_member(tar, member, root)
    except (tarfile.TarError, zstandard.ZstdError) as error:
        msg = "invalid compressed tar archive"
        raise ArchiveError(msg) from error


def main() -> None:
    """Create or extract an archive from the command line."""
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create")
    create.add_argument("source", type=Path)
    create.add_argument("output", type=Path)
    create.add_argument("--prefix", default="")
    extract = commands.add_parser("extract")
    extract.add_argument("archive", type=Path)
    extract.add_argument("destination", type=Path)
    args = parser.parse_args()
    if args.command == "create":
        create_archive(args.source, args.output, prefix=args.prefix)
    else:
        extract_archive(args.archive, args.destination)


if __name__ == "__main__":
    main()
