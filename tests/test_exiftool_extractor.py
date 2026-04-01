from pathlib import Path
import json

from photo_archive.extractors.exiftool_extractor import ExifToolExtractor


def test_extract_batch_splits_when_command_too_long(monkeypatch) -> None:
    extractor = ExifToolExtractor(batch_size=10)
    paths = [Path(f"/photos/file_{idx}.jpg") for idx in range(4)]

    call_sizes: list[int] = []

    def _fake_run(command, capture_output, text, check, timeout):  # noqa: ANN001
        # Every invocation reaches this function from _extract_batch.
        batch_count = max(0, len(command) - 6)
        call_sizes.append(batch_count)
        if batch_count > 1:
            err = OSError("[WinError 206] The filename or extension is too long")
            err.winerror = 206  # type: ignore[attr-defined]
            raise err

        source_path = command[-1]
        payload = json.dumps([{"SourceFile": source_path}]).encode("utf-8")

        class _Done:
            returncode = 0
            stdout = payload
            stderr = b""

        return _Done()

    monkeypatch.setattr("photo_archive.extractors.exiftool_extractor.subprocess.run", _fake_run)

    result = extractor._extract_batch(paths)

    assert len(result) == 4
    assert all(item.status == "success" for item in result.values())
    # Confirms recursive splitting happened, then singleton execution.
    assert any(size > 1 for size in call_sizes)
    assert call_sizes.count(1) == 4
