from __future__ import annotations

from collections.abc import Iterable

DEFAULT_SUPPORTED_EXTENSIONS: tuple[str, ...] = (
    ".jpg",
    ".jpeg",
    ".png",
    ".heic",
    ".tif",
    ".tiff",
    ".webp",
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
