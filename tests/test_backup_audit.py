from pathlib import Path

from photo_archive.backup_audit import run_backup_audit
from photo_archive.models import BackupAuditFileRecord


def test_backup_audit_matches_by_hash_and_relative_path() -> None:
    primary_root = Path("/primary")
    backup_root = Path("/backup")

    primary_files = [
        BackupAuditFileRecord(
            file_id="p1",
            path="/primary/a/photo1.jpg",
            size_bytes=100,
            content_sha256="hash-1",
        ),
        BackupAuditFileRecord(
            file_id="p2",
            path="/primary/a/photo2.jpg",
            size_bytes=100,
            content_sha256=None,
        ),
        BackupAuditFileRecord(
            file_id="p3",
            path="/primary/a/photo3.jpg",
            size_bytes=100,
            content_sha256="hash-3",
        ),
        BackupAuditFileRecord(
            file_id="p4",
            path="/primary/a/photo4.jpg",
            size_bytes=100,
            content_sha256=None,
        ),
    ]
    backup_files = [
        BackupAuditFileRecord(
            file_id="b1",
            path="/backup/x/renamed_photo1.jpg",
            size_bytes=120,
            content_sha256="hash-1",
        ),
        BackupAuditFileRecord(
            file_id="b2",
            path="/backup/a/photo2.jpg",
            size_bytes=100,
            content_sha256=None,
        ),
    ]

    result = run_backup_audit(
        primary_scan_id="scan_primary",
        backup_scan_id="scan_backup",
        primary_root=primary_root,
        backup_root=backup_root,
        primary_files=primary_files,
        backup_files=backup_files,
        limit=10,
    )

    assert result.summary["primary_active_files"] == 4
    assert result.summary["backup_active_files"] == 2
    assert result.summary["backed_up_files"] == 2
    assert result.summary["missing_in_backup_total"] == 2
    assert result.summary["missing_hashed"] == 1
    assert result.summary["missing_unhashed"] == 1
    assert [item.file_id for item in result.missing_files] == ["p3", "p4"]
