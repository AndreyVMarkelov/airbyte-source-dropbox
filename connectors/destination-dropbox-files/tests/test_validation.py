from pathlib import Path

import pytest

from destination_dropbox_files.validation import (
    FileReferenceValidationError,
    validate_propagation_operation,
    validate_staged_file,
)


def test_validates_local_reference_and_preserves_relative_path(tmp_path: Path) -> None:
    staged = tmp_path / "file.bin"
    staged.write_bytes(b"data")

    result = validate_staged_file(
        staging_file_url=staged.as_uri(),
        relative_path="nested/file.bin",
        file_size_bytes=4,
        root_path="/Exports",
        sha256=None,
    )

    assert result.destination_path == "/Exports/nested/file.bin"


def test_rejects_missing_or_mismatched_staged_file(tmp_path: Path) -> None:
    missing = (tmp_path / "missing.bin").as_uri()
    with pytest.raises(FileReferenceValidationError):
        validate_staged_file(
            staging_file_url=missing,
            relative_path="file.bin",
            file_size_bytes=1,
            root_path="",
            sha256=None,
        )


def test_validates_move_and_delete_control_records() -> None:
    move = validate_propagation_operation(
        {
            "operation": "move",
            "file_id": "id:file",
            "old_path": "old/file.pdf",
            "old_content_hash": "old-hash",
            "new_path": "new/file.pdf",
            "new_content_hash": "new-hash",
        }
    )
    delete = validate_propagation_operation(
        {
            "operation": "delete",
            "file_id": "id:file",
            "old_path": "old/file.pdf",
            "old_content_hash": "old-hash",
        }
    )

    assert move.new_path == "new/file.pdf"
    assert delete.new_path is None


def test_rejects_unsafe_propagation_paths() -> None:
    with pytest.raises(FileReferenceValidationError, match="invalid"):
        validate_propagation_operation(
            {
                "operation": "delete",
                "file_id": "id:file",
                "old_path": "../outside.pdf",
                "old_content_hash": "hash",
            }
        )
