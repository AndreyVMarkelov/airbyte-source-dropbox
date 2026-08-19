from __future__ import annotations

import base64
import binascii
import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
RFC3339_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$")
CONFLICT_POLICIES = frozenset({"overwrite", "fail"})


class RecordValidationError(ValueError):
    """Raised when an incoming Airbyte record violates the file contract."""


class DestinationConfigurationError(ValueError):
    """Raised when destination configuration is invalid."""


@dataclass(frozen=True)
class ValidatedFileRecord:
    destination_path: str
    content: bytes
    sha256: str | None
    modified_at: datetime | None


def normalize_root_path(root_path: str) -> str:
    if not isinstance(root_path, str):
        raise RecordValidationError("root_path must be a string.")
    if root_path == "":
        return ""
    if "\\" in root_path or "//" in root_path or not root_path.startswith("/"):
        raise RecordValidationError(
            "root_path must be an absolute POSIX path without repeated separators."
        )
    segments = root_path.split("/")[1:]
    if not segments or any(segment in {"", ".", ".."} for segment in segments):
        raise RecordValidationError("root_path contains an invalid path segment.")
    return root_path.rstrip("/")


def normalize_conflict_policy(value: Any) -> str:
    if value not in CONFLICT_POLICIES:
        raise DestinationConfigurationError("conflict_policy must be either 'overwrite' or 'fail'.")
    return value


def validate_record(
    record: Mapping[str, Any], *, root_path: str, max_file_size_bytes: int
) -> ValidatedFileRecord:
    if not isinstance(record, Mapping):
        raise RecordValidationError("record data must be an object.")
    path = _required_string(record, "path")
    destination_path = _join_destination_path(root_path, path)
    content = _decode_content(_required_string(record, "content_base64"), max_file_size_bytes)
    supplied_sha256 = record.get("sha256")
    if supplied_sha256 is not None:
        if not isinstance(supplied_sha256, str) or not SHA256_PATTERN.fullmatch(supplied_sha256):
            raise RecordValidationError("sha256 must be a lowercase 64-character SHA-256 digest.")
        if hashlib.sha256(content).hexdigest() != supplied_sha256:
            raise RecordValidationError("sha256 does not match decoded content.")
    modified_at = _parse_modified_at(record.get("modified_at"))
    return ValidatedFileRecord(destination_path, content, supplied_sha256, modified_at)


def _required_string(record: Mapping[str, Any], field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value:
        raise RecordValidationError(f"{field} must be a non-empty string.")
    return value


def _join_destination_path(root_path: str, path: str) -> str:
    if path.startswith("/") or "\\" in path or "//" in path:
        raise RecordValidationError(
            "path must be a relative POSIX path without repeated separators."
        )
    segments = path.split("/")
    if any(segment in {"", ".", ".."} for segment in segments):
        raise RecordValidationError("path contains an invalid path segment.")
    return f"{root_path}/{path}" if root_path else f"/{path}"


def _decode_content(content_base64: str, max_file_size_bytes: int) -> bytes:
    try:
        content = base64.b64decode(content_base64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RecordValidationError("content_base64 must be valid RFC 4648 base64.") from exc
    if len(content) > max_file_size_bytes:
        raise RecordValidationError("decoded content exceeds max_file_size_mb.")
    return content


def _parse_modified_at(value: Any) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RecordValidationError("modified_at must be an RFC 3339 timestamp string.")
    if not RFC3339_PATTERN.fullmatch(value):
        raise RecordValidationError(
            "modified_at must be a valid RFC 3339 timestamp with a timezone."
        )
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise RecordValidationError("modified_at must be a valid RFC 3339 timestamp.") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RecordValidationError("modified_at must include a timezone.")
    return parsed.astimezone(UTC)
