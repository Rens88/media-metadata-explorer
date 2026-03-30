from __future__ import annotations

from collections.abc import Iterable

DEFAULT_IMAGE_EXTENSIONS: tuple[str, ...] = (
    ".jpg",
    ".jpeg",
    ".png",
    ".heic",
    ".tif",
    ".tiff",
    ".webp",
)

DEFAULT_VIDEO_EXTENSIONS: tuple[str, ...] = (
    ".mp4",
    ".mov",
    ".avi",
    ".mkv",
)

DEFAULT_SUPPORTED_EXTENSIONS: tuple[str, ...] = (
    *DEFAULT_IMAGE_EXTENSIONS,
    *DEFAULT_VIDEO_EXTENSIONS,
)


def normalize_extensions(extensions: Iterable[str] | None) -> set[str]:
    """Normalize user-provided extension strings into lowercase `.ext` tokens."""
    if not extensions:
        return set(DEFAULT_SUPPORTED_EXTENSIONS)

    normalized: set[str] = set()
    for ext in extensions:
        ext_value = ext.strip().lower()
        if not ext_value:
            continue
        if not ext_value.startswith("."):
            ext_value = f".{ext_value}"
        normalized.add(ext_value)
    return normalized or set(DEFAULT_SUPPORTED_EXTENSIONS)


def media_type_for_extension(extension: str, supported_extensions: set[str]) -> str:
    ext_value = extension.strip().lower()
    if ext_value not in supported_extensions:
        return "unknown"
    if ext_value in DEFAULT_VIDEO_EXTENSIONS:
        return "video"
    return "image"
