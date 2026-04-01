from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from photo_archive.models import BackupAuditFileRecord


@dataclass(slots=True)
class BackupAuditResult:
    primary_scan_id: str
    backup_scan_id: str
    summary: dict[str, int]
    missing_files: list[BackupAuditFileRecord]


def run_backup_audit(
    *,
    primary_scan_id: str,
    backup_scan_id: str,
    primary_root: Path,
    backup_root: Path,
    primary_files: list[BackupAuditFileRecord],
    backup_files: list[BackupAuditFileRecord],
    limit: int = 200,
) -> BackupAuditResult:
    backup_hashes = {item.content_sha256 for item in backup_files if item.content_sha256}
    backup_relative_paths = {
        rel
        for rel in (_relative_path_key(item.path, backup_root) for item in backup_files)
        if rel
    }

    hashed_primary = 0
    unhashed_primary = 0
    backed_up_count = 0
    missing_hashed = 0
    missing_unhashed = 0
    missing_all: list[BackupAuditFileRecord] = []

    for item in primary_files:
        rel_key = _relative_path_key(item.path, primary_root)
        has_hash = bool(item.content_sha256)
        if has_hash:
            hashed_primary += 1
            if item.content_sha256 in backup_hashes:
                backed_up_count += 1
                continue
        else:
            unhashed_primary += 1

        if rel_key and rel_key in backup_relative_paths:
            backed_up_count += 1
            continue

        if has_hash:
            missing_hashed += 1
        else:
            missing_unhashed += 1
        missing_all.append(item)

    visible_missing = missing_all[: max(1, int(limit))]
    summary = {
        "primary_active_files": len(primary_files),
        "backup_active_files": len(backup_files),
        "primary_hashed_files": hashed_primary,
        "primary_unhashed_files": unhashed_primary,
        "backed_up_files": backed_up_count,
        "missing_in_backup_total": len(missing_all),
        "missing_hashed": missing_hashed,
        "missing_unhashed": missing_unhashed,
        "missing_shown": len(visible_missing),
    }
    return BackupAuditResult(
        primary_scan_id=primary_scan_id,
        backup_scan_id=backup_scan_id,
        summary=summary,
        missing_files=visible_missing,
    )


def format_backup_audit_summary(
    result: BackupAuditResult,
    *,
    primary_root: Path,
    backup_root: Path,
) -> str:
    summary = result.summary
    lines = [
        "Backup audit summary",
        f"  primary_root: {primary_root}",
        f"  backup_root: {backup_root}",
        f"  primary_scan_id: {result.primary_scan_id}",
        f"  backup_scan_id: {result.backup_scan_id}",
        f"  primary_active_files: {summary.get('primary_active_files', 0)}",
        f"  backup_active_files: {summary.get('backup_active_files', 0)}",
        f"  primary_hashed_files: {summary.get('primary_hashed_files', 0)}",
        f"  primary_unhashed_files: {summary.get('primary_unhashed_files', 0)}",
        f"  backed_up_files: {summary.get('backed_up_files', 0)}",
        f"  missing_in_backup_total: {summary.get('missing_in_backup_total', 0)}",
        f"  missing_hashed: {summary.get('missing_hashed', 0)}",
        f"  missing_unhashed: {summary.get('missing_unhashed', 0)}",
        "",
        f"Missing files (showing up to {summary.get('missing_shown', 0)})",
    ]
    if not result.missing_files:
        lines.append("  none")
    else:
        for item in result.missing_files:
            hash_text = item.content_sha256 or "[no_hash]"
            size_text = str(item.size_bytes) if item.size_bytes is not None else "[no_size]"
            lines.append(f"  {item.path} | size={size_text} | sha256={hash_text}")
    return "\n".join(lines)


def _relative_path_key(path: str, root: Path) -> str | None:
    root_resolved = root.expanduser().resolve(strict=False)
    path_resolved = Path(path).expanduser().resolve(strict=False)
    try:
        rel = path_resolved.relative_to(root_resolved)
    except ValueError:
        return None
    return rel.as_posix().lower()
