from datetime import datetime

from photo_archive.extractors.filename_parser import parse_filename_datetime


def test_parse_img_pattern() -> None:
    result = parse_filename_datetime("IMG_20220304_153012.jpg")
    assert result.parsed_datetime == datetime(2022, 3, 4, 15, 30, 12)
    assert result.parsed_pattern == "img_yyyymmdd_hhmmss"
    assert result.parse_confidence == 0.95


def test_parse_pxl_pattern() -> None:
    result = parse_filename_datetime("PXL_20240201_102030123.jpeg")
    assert result.parsed_datetime == datetime(2024, 2, 1, 10, 20, 30)
    assert result.parsed_pattern == "pxl_yyyymmdd_hhmmssmmm"
    assert result.parse_confidence == 0.95


def test_parse_whatsapp_pattern() -> None:
    result = parse_filename_datetime("WhatsApp Image 2022-03-05 at 19.22.10.png")
    assert result.parsed_datetime == datetime(2022, 3, 5, 19, 22, 10)
    assert result.parsed_pattern == "whatsapp_image"
    assert result.parse_confidence == 0.90


def test_parse_unknown_pattern_returns_nulls() -> None:
    result = parse_filename_datetime("vacation_photo_final.jpg")
    assert result.parsed_datetime is None
    assert result.parsed_pattern is None
    assert result.parse_confidence is None
