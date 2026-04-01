from pathlib import Path

from photo_archive.hash_utils import hash_file_sha256


def test_hash_file_sha256_returns_expected_digest(tmp_path: Path) -> None:
    source = tmp_path / "a.bin"
    source.write_bytes(b"abc")

    digest = hash_file_sha256(source)

    assert digest == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
