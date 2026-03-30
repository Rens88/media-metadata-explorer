from pathlib import Path

from photo_archive.extractors.ffprobe_extractor import FFprobeExtractor


class _DummyCompletedProcess:
    def __init__(self, *, returncode: int, stdout: bytes, stderr: bytes) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_ffprobe_extractor_not_found(monkeypatch, tmp_path: Path) -> None:
    file_path = tmp_path / "video.mp4"
    file_path.write_bytes(b"dummy")

    monkeypatch.setattr("photo_archive.extractors.ffprobe_extractor.shutil.which", lambda _: None)
    extractor = FFprobeExtractor(executable="ffprobe")
    results = extractor.extract([file_path])

    resolved = str(file_path.resolve(strict=False))
    assert resolved in results
    assert results[resolved].status == "failed"
    assert results[resolved].error is not None
    assert "ffprobe_not_found" in results[resolved].error


def test_ffprobe_extractor_success(monkeypatch, tmp_path: Path) -> None:
    file_path = tmp_path / "video.mp4"
    file_path.write_bytes(b"dummy")

    payload = (
        b'{"format":{"duration":"12.5","bit_rate":"1200000"},'
        b'"streams":[{"codec_type":"video","codec_name":"h264","avg_frame_rate":"30000/1001"}]}'
    )

    monkeypatch.setattr("photo_archive.extractors.ffprobe_extractor.shutil.which", lambda _: "/usr/bin/ffprobe")
    monkeypatch.setattr(
        "photo_archive.extractors.ffprobe_extractor.subprocess.run",
        lambda *args, **kwargs: _DummyCompletedProcess(returncode=0, stdout=payload, stderr=b""),
    )
    extractor = FFprobeExtractor()
    results = extractor.extract([file_path])

    resolved = str(file_path.resolve(strict=False))
    assert results[resolved].status == "success"
    assert results[resolved].raw_metadata is not None
    assert results[resolved].raw_metadata.get("format", {}).get("duration") == "12.5"


def test_ffprobe_extractor_invalid_json(monkeypatch, tmp_path: Path) -> None:
    file_path = tmp_path / "video.mp4"
    file_path.write_bytes(b"dummy")

    monkeypatch.setattr("photo_archive.extractors.ffprobe_extractor.shutil.which", lambda _: "/usr/bin/ffprobe")
    monkeypatch.setattr(
        "photo_archive.extractors.ffprobe_extractor.subprocess.run",
        lambda *args, **kwargs: _DummyCompletedProcess(returncode=0, stdout=b"{bad", stderr=b""),
    )
    extractor = FFprobeExtractor()
    results = extractor.extract([file_path])

    resolved = str(file_path.resolve(strict=False))
    assert results[resolved].status == "failed"
    assert results[resolved].error == "ffprobe_invalid_json"
