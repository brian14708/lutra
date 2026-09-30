from pathlib import Path

import pytest
from lutra.archive import ArchiveError, create_archive, extract_archive, read_archive


def test_archive_roundtrip(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "one.txt").write_bytes(b"one")
    (source / "empty.txt").write_bytes(b"")
    (source / "sub").mkdir()
    (source / "sub" / "two.txt").write_bytes(b"two")
    (source / "sub" / "link.txt").symlink_to("two.txt")
    long_name = "x" * 110
    (source / long_name).write_bytes(b"long name")
    archive = tmp_path / "archive.tar.zst"
    create_archive(source, archive)
    assert read_archive(archive, "one.txt") == b"one"
    assert read_archive(archive, "empty.txt") == b""
    assert read_archive(archive, "sub/two.txt") == b"two"
    assert read_archive(archive, long_name) == b"long name"

    destination = tmp_path / "destination"
    extract_archive(archive, destination)
    assert (destination / "one.txt").read_bytes() == b"one"
    assert (destination / "empty.txt").read_bytes() == b""
    assert (destination / "sub" / "two.txt").read_bytes() == b"two"
    assert (destination / "sub" / "link.txt").is_symlink()
    assert (destination / "sub" / "link.txt").read_text() == "two"
    assert (destination / long_name).read_bytes() == b"long name"


def test_archive_symlinks(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "directory").mkdir()
    (source / "directory" / "file").write_text("contents")
    (source / "alias").symlink_to("directory", target_is_directory=True)
    (source / "dangling").symlink_to("missing")
    (source / "python").symlink_to("/usr/bin/python3")
    archive = tmp_path / "archive.tar.zst"
    create_archive(source, archive)
    destination = tmp_path / "destination"
    extract_archive(archive, destination)
    assert (destination / "alias").is_symlink()
    assert (destination / "alias" / "file").read_text() == "contents"
    assert str((destination / "dangling").readlink()) == "missing"
    assert str((destination / "python").readlink()) == "/usr/bin/python3"
    assert read_archive(archive, "directory/file") == b"contents"
    with pytest.raises(KeyError):
        read_archive(archive, "python")


@pytest.mark.parametrize("link", ["../outside", "/outside", "/usr/../../outside"])
def test_archive_rejects_escaping_symlink(tmp_path: Path, link: str) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "link").symlink_to(link)
    archive = tmp_path / "archive.tar.zst"
    create_archive(source, archive)
    with pytest.raises(ArchiveError, match="symlink escapes"):
        extract_archive(archive, tmp_path / "destination")


def test_archive_rejects_existing_external_symlink(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "directory").mkdir()
    (source / "directory" / "file").write_text("contents")
    archive = tmp_path / "archive.tar.zst"
    create_archive(source, archive)
    destination = tmp_path / "destination"
    destination.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (destination / "directory").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ArchiveError, match="path escapes"):
        extract_archive(archive, destination)
    assert not (outside / "file").exists()


def test_archive_rejects_oversized_file(tmp_path: Path) -> None:
    source = tmp_path / "oversized"
    with source.open("wb") as file:
        file.truncate((2 << 30) + 1)
    with pytest.raises(ArchiveError, match="size or entry limit"):
        create_archive(source, tmp_path / "archive.tar.zst")
