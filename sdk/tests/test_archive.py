from pathlib import Path

from lutra.archive import create_archive, extract_archive, read_archive


def test_archive_roundtrip(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "one.txt").write_bytes(b"one")
    (source / "empty.txt").write_bytes(b"")
    (source / "sub").mkdir()
    (source / "sub" / "two.txt").write_bytes(b"two")
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
    assert (destination / long_name).read_bytes() == b"long name"
