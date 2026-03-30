from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from photo_archive.models import FilenameParseRecord

IMG_PATTERN = re.compile(r"(?i)\bIMG[_-]?(\d{8})[_-]?(\d{6})\b")
PXL_PATTERN = re.compile(r"(?i)\bPXL[_-]?(\d{8})[_-]?(\d{6})(\d{3})?\b")
GENERIC_DATETIME_PATTERN = re.compile(
    r"\b(\d{4})[-_](\d{2})[-_](\d{2})[ _T](\d{2})[.:-](\d{2})[.:-](\d{2})\b"
)
WHATSAPP_PATTERN = re.compile(
    r"(?i)\bWhatsApp Image (\d{4})-(\d{2})-(\d{2}) at (\d{2})[.:-](\d{2})[.:-](\d{2})\b"
)


def parse_filename_datetime(filename: str) -> FilenameParseRecord:
    """Parse known filename datetime patterns with explicit confidence."""
    stem = Path(filename).stem

    match = IMG_PATTERN.search(stem)
    if match:
        date_part, time_part = match.groups()
        parsed = _parse_yyyymmdd_hhmmss(date_part, time_part)
        if parsed:
            return FilenameParseRecord(
                parsed_datetime=parsed,
                parsed_pattern="img_yyyymmdd_hhmmss",
                parse_confidence=0.95,
            )

    match = PXL_PATTERN.search(stem)
    if match:
        date_part, time_part, _millis = match.groups()
        parsed = _parse_yyyymmdd_hhmmss(date_part, time_part)
        if parsed:
            return FilenameParseRecord(
                parsed_datetime=parsed,
                parsed_pattern="pxl_yyyymmdd_hhmmssmmm",
                parse_confidence=0.95,
            )

    match = WHATSAPP_PATTERN.search(stem)
    if match:
        year, month, day, hour, minute, second = match.groups()
        parsed = _parse_parts(year, month, day, hour, minute, second)
        if parsed:
            return FilenameParseRecord(
                parsed_datetime=parsed,
                parsed_pattern="whatsapp_image",
                parse_confidence=0.90,
            )

    match = GENERIC_DATETIME_PATTERN.search(stem)
    if match:
        parsed = _parse_parts(*match.groups())
        if parsed:
            return FilenameParseRecord(
                parsed_datetime=parsed,
                parsed_pattern="generic_datetime",
                parse_confidence=0.70,
            )

    return FilenameParseRecord()


def _parse_yyyymmdd_hhmmss(date_part: str, time_part: str) -> datetime | None:
    return _parse_parts(
        date_part[0:4],
        date_part[4:6],
        date_part[6:8],
        time_part[0:2],
        time_part[2:4],
        time_part[4:6],
    )


def _parse_parts(
    year: str, month: str, day: str, hour: str, minute: str, second: str
) -> datetime | None:
    try:
        return datetime(
            int(year), int(month), int(day), int(hour), int(minute), int(second)
        )
    except ValueError:
        return None
