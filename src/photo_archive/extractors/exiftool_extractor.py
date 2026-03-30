from __future__ import annotations

import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

from photo_archive.models import ExtractionResult

LOGGER = logging.getLogger(__name__)


class ExifToolExtractor:
    def __init__(
        self,
        executable: str = "exiftool",
        batch_size: int = 200,
        timeout_seconds: int = 180,
    ) -> None:
        self.executable = executable
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds

    def is_available(self) -> bool:
        return shutil.which(self.executable) is not None

    def extract(self, file_paths: list[Path]) -> dict[str, ExtractionResult]:
        """Extract raw metadata for files in batches, returning per-file status."""
        if not file_paths:
            return {}

        if not self.is_available():
            error = f"exiftool_not_found: '{self.executable}' is not available on PATH"
            LOGGER.error(error)
            return {
                str(path.resolve(strict=False)): ExtractionResult(
                    path=str(path.resolve(strict=False)),
                    status="failed",
                    error=error,
                )
                for path in file_paths
            }

        results: dict[str, ExtractionResult] = {}
        for idx in range(0, len(file_paths), self.batch_size):
            batch = file_paths[idx : idx + self.batch_size]
            batch_results = self._extract_batch(batch)
            results.update(batch_results)
        return results

    def _extract_batch(self, batch: list[Path]) -> dict[str, ExtractionResult]:
        command = [
            self.executable,
            "-j",
            "-n",
            "-G1",
            "-api",
            "largefilesupport=1",
            *[str(path.resolve(strict=False)) for path in batch],
        ]
        try:
            process = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            error = "exiftool_timeout"
            return self._batch_failure(batch, error, stderr="process timed out")
        except OSError as exc:
            error = f"exiftool_exec_failed: {exc}"
            return self._batch_failure(batch, error)

        entries: list[dict[str, Any]] = []
        stdout = process.stdout.strip()
        if stdout:
            try:
                raw = json.loads(stdout)
                if isinstance(raw, list):
                    entries = [entry for entry in raw if isinstance(entry, dict)]
                else:
                    return self._batch_failure(
                        batch,
                        "exiftool_invalid_json",
                        stderr="expected list payload from exiftool",
                    )
            except json.JSONDecodeError as exc:
                return self._batch_failure(
                    batch,
                    "exiftool_invalid_json",
                    stderr=f"JSON parse error: {exc}",
                )

        by_source_file: dict[str, dict[str, Any]] = {}
        for entry in entries:
            source_file = entry.get("SourceFile")
            if isinstance(source_file, str):
                by_source_file[str(Path(source_file).resolve(strict=False))] = entry

        batch_results: dict[str, ExtractionResult] = {}
        for path in batch:
            resolved = str(path.resolve(strict=False))
            raw_metadata = by_source_file.get(resolved)
            if raw_metadata is None:
                stderr = process.stderr.strip() or None
                error = "exiftool_missing_output"
                if process.returncode != 0:
                    error = f"exiftool_failed_rc_{process.returncode}"
                batch_results[resolved] = ExtractionResult(
                    path=resolved,
                    status="failed",
                    error=error,
                    stderr=stderr,
                )
            else:
                batch_results[resolved] = ExtractionResult(
                    path=resolved,
                    status="success",
                    raw_metadata=raw_metadata,
                    stderr=process.stderr.strip() or None,
                )
        return batch_results

    def _batch_failure(
        self, batch: list[Path], error: str, stderr: str | None = None
    ) -> dict[str, ExtractionResult]:
        output: dict[str, ExtractionResult] = {}
        for path in batch:
            resolved = str(path.resolve(strict=False))
            output[resolved] = ExtractionResult(
                path=resolved,
                status="failed",
                error=error,
                stderr=stderr,
            )
        return output
