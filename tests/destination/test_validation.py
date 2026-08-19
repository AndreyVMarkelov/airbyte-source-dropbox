import base64
import hashlib
from datetime import UTC

import pytest

from destination_dropbox.validation import (
    DestinationConfigurationError,
    RecordValidationError,
    normalize_conflict_policy,
    normalize_root_path,
    validate_record,
)


def test_normalize_conflict_policy_accepts_supported_values_only() -> None:
    assert normalize_conflict_policy("overwrite") == "overwrite"
    assert normalize_conflict_policy("fail") == "fail"
    with pytest.raises(DestinationConfigurationError, match="conflict_policy"):
        normalize_conflict_policy("rename")


def _record(content: bytes = b"report", **overrides: object) -> dict[str, object]:
    record: dict[str, object] = {
        "path": "folder/report.pdf",
        "content_base64": base64.b64encode(content).decode(),
    }
    record.update(overrides)
    return record


def test_valid_record_joins_root_verifies_sha256_and_normalizes_timestamp() -> None:
    content = b"report"
    result = validate_record(
        _record(
            content,
            sha256=hashlib.sha256(content).hexdigest(),
            modified_at="2026-08-18T14:00:00+02:00",
        ),
        root_path="/Exports",
        max_file_size_bytes=10,
    )

    assert result.destination_path == "/Exports/folder/report.pdf"
    assert result.content == content
    assert result.modified_at is not None
    assert result.modified_at.tzinfo == UTC
    assert result.modified_at.hour == 12


@pytest.mark.parametrize(
    "path",
    [
        "/report.pdf",
        "folder//report.pdf",
        "folder\\report.pdf",
        "folder/../report.pdf",
        "./report.pdf",
    ],
)
def test_record_rejects_ambiguous_or_escaping_path(path: str) -> None:
    with pytest.raises(RecordValidationError, match="path"):
        validate_record(_record(path=path), root_path="", max_file_size_bytes=10)


@pytest.mark.parametrize("root_path", ["Exports", "/Exports//archive", "/Exports/../archive"])
def test_root_path_rejects_non_canonical_paths(root_path: str) -> None:
    with pytest.raises(RecordValidationError, match="root_path"):
        normalize_root_path(root_path)


def test_base64_is_strict_and_decoded_limit_is_enforced() -> None:
    with pytest.raises(RecordValidationError, match="base64"):
        validate_record(_record(content_base64="not base64!"), root_path="", max_file_size_bytes=10)

    result = validate_record(_record(b"x" * 10), root_path="", max_file_size_bytes=10)
    assert result.content == b"x" * 10
    with pytest.raises(RecordValidationError, match="exceeds"):
        validate_record(_record(b"x" * 11), root_path="", max_file_size_bytes=10)


def test_sha256_and_timestamp_validation() -> None:
    with pytest.raises(RecordValidationError, match="sha256"):
        validate_record(_record(sha256="ABC"), root_path="", max_file_size_bytes=10)
    with pytest.raises(RecordValidationError, match="does not match"):
        validate_record(_record(sha256="0" * 64), root_path="", max_file_size_bytes=10)
    with pytest.raises(RecordValidationError, match="timezone"):
        validate_record(
            _record(modified_at="2026-08-18T12:00:00"), root_path="", max_file_size_bytes=10
        )
    with pytest.raises(RecordValidationError, match="RFC 3339"):
        validate_record(
            _record(modified_at="2026-08-18 12:00:00Z"), root_path="", max_file_size_bytes=10
        )
