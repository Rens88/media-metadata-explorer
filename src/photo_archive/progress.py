from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import os
import shutil
from time import perf_counter
import textwrap


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    rem = seconds - (minutes * 60)
    return f"{minutes}m {rem:.1f}s"


def _status_code(status: str) -> str:
    mapping = {
        "INFO": "IN",
        "START": "ST",
        "DONE": "OK",
    }
    return mapping.get(status, status[:2].upper())


def _render_width() -> int:
    env_value = os.environ.get("PHOTO_ARCHIVE_PROGRESS_WIDTH")
    if env_value:
        try:
            forced = int(env_value)
            if forced >= 60:
                return forced
        except ValueError:
            pass
    terminal_columns = shutil.get_terminal_size(fallback=(120, 20)).columns
    # Keep lines comfortably readable on laptops even when terminal is very wide.
    return max(80, min(terminal_columns, 120))


def print_progress_line(
    *,
    topic: str,
    purpose: str | None = None,
    expectation: str | None = None,
    status: str = "INFO",
    duration_seconds: float | None = None,
    details: str | None = None,
) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    prefix = f"[{timestamp}][{_status_code(status)}][{topic}]"
    parts: list[str] = []
    if purpose:
        parts.append(f"p={purpose}")
    if expectation:
        parts.append(f"x={expectation}")
    if duration_seconds is not None:
        parts.append(f"t={format_duration(duration_seconds)}")
    if details:
        parts.append(f"d={details}")

    if not parts:
        print(prefix)
        return

    payload = " | ".join(parts)
    line_width = _render_width()
    wrapped = textwrap.fill(
        payload,
        width=line_width,
        initial_indent=f"{prefix} ",
        subsequent_indent=" " * (len(prefix) + 1),
        break_long_words=False,
        break_on_hyphens=False,
    )
    print(wrapped)


@dataclass(slots=True)
class ProgressPrinter:
    enabled: bool = True
    _active_stages: dict[str, tuple[float, str, str]] = field(default_factory=dict)

    def info(self, topic: str, purpose: str, expectation: str, details: str | None = None) -> None:
        if not self.enabled:
            return
        print_progress_line(
            topic=topic,
            purpose=purpose,
            expectation=expectation,
            status="INFO",
            details=details,
        )

    def start(self, topic: str, purpose: str, expectation: str, details: str | None = None) -> None:
        self._active_stages[topic] = (perf_counter(), purpose, expectation)
        if not self.enabled:
            return
        print_progress_line(
            topic=topic,
            purpose=purpose,
            expectation=expectation,
            status="START",
            details=details,
        )

    def done(self, topic: str, details: str | None = None) -> float:
        stage_info = self._active_stages.pop(topic, None)
        if stage_info is None:
            if not self.enabled:
                return 0.0
            print_progress_line(
                topic=topic,
                purpose="finish stage",
                status="DONE",
                details=details,
            )
            return 0.0

        started_at, purpose, expectation = stage_info
        elapsed = perf_counter() - started_at
        if not self.enabled:
            return elapsed
        print_progress_line(
            topic=topic,
            purpose=None,
            expectation=None,
            status="DONE",
            duration_seconds=elapsed,
            details=details,
        )
        return elapsed
