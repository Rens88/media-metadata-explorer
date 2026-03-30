from __future__ import annotations

import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

from photo_archive.models import ExtractionResult

LOGGER = logging.getLogger(__name__)


class FFprobeExtractor:
    def __init__(
        self,
        executable: str = "ffprobe",
        timeout_seconds: int = 120,
    ) -> None:
        self.executable = executable
        self.timeout_seconds = timeout_seconds

    def is_available(self) -> bool:
        return shutil.which(self.executable) is not None

    def extract(self, file_paths: list[Path]) -> dict[str, ExtractionResult]:
        if not file_paths:
            return {}

        if not self.is_available():
            error = f"ffprobe_not_found: '{self.executable}' is not available on PATH"
            LOGGER.error(error)
            return {
                str(path.resolve(strict=False)): ExtractionResult(
                    path=str(path.resolve(strict=False)),
                    status="failed",
                    error=error,
                )
                for path in file_paths
            }

        output: dict[str, ExtractionResult] = {}
        for path in file_paths:
            result = self._extract_one(path)
            output[result.path] = result
        return output

    def _extract_one(self, path: Path) -> ExtractionResult:
        resolved = str(path.resolve(strict=False))
        command = [
            self.executable,
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            resolved,
        ]
        try:
            process = subprocess.run(
                command,
                capture_output=True,
                text=False,
                check=False,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            return ExtractionResult(
                path=resolved,
                status="failed",
                error="ffprobe_timeout",
                stderr="process timed out",
            )
        except OSError as exc:
            return ExtractionResult(
                path=resolved,
                status="failed",
                error=f"ffprobe_exec_failed: {exc}",
            )

        stdout = _decode_subprocess_bytes(process.stdout).strip()
        stderr_text = _decode_subprocess_bytes(process.stderr).strip() or None

        if not stdout:
            if process.returncode != 0:
                return ExtractionResult(
                    path=resolved,
                    status="failed",
                    error=f"ffprobe_failed_rc_{process.returncode}",
                    stderr=stderr_text,
                )
            return ExtractionResult(
                path=resolved,
                status="failed",
                error="ffprobe_missing_output",
                stderr=stderr_text,
            )

        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            return ExtractionResult(
                path=resolved,
                status="failed",
                error="ffprobe_invalid_json",
                stderr=f"JSON parse error: {exc}",
            )

        if not isinstance(payload, dict):
            return ExtractionResult(
                path=resolved,
                status="failed",
                error="ffprobe_invalid_json",
                stderr="expected object payload from ffprobe",
            )

        if process.returncode != 0:
            return ExtractionResult(
                path=resolved,
                status="failed",
                error=f"ffprobe_failed_rc_{process.returncode}",
                stderr=stderr_text,
            )

        return ExtractionResult(
            path=resolved,
            status="success",
            raw_metadata=payload,
            stderr=stderr_text,
        )


def _decode_subprocess_bytes(value: bytes | None) -> str:
    if value is None:
        return ""
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value.decode("utf-8", errors="replace")
