from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlparse


class FileReferenceValidationError(ValueError):
    pass


@dataclass(frozen=True)
class PropagationOperation:
    kind: str
    file_id: str
    old_path: str
    old_content_hash: str
    new_path: str | None = None
    new_content_hash: str | None = None


@dataclass(frozen=True)
class StagedFile:
    path: Path
    destination_path: str
    size: int
    sha256: str | None
    client_modified: datetime | None = None


def normalize_metadata_policy(value: object) -> str:
    if value not in {"preserve", "ignore"}:
        raise FileReferenceValidationError("metadata_policy must be preserve or ignore.")
    return str(value)


def normalize_root_path(value: object) -> str:
    if not isinstance(value, str):
        raise FileReferenceValidationError("root_path must be a string.")
    if value == "":
        return ""
    if not value.startswith("/") or "\\" in value or "//" in value:
        raise FileReferenceValidationError("root_path must be an absolute POSIX path.")
    if any(segment in {"", ".", ".."} for segment in value.split("/")[1:]):
        raise FileReferenceValidationError("root_path contains an invalid path segment.")
    return value.rstrip("/")


def validate_staged_file(
    *, staging_file_url: str | None, relative_path: object, file_size_bytes: object, root_path: str,
    sha256: object, client_modified: object = None, metadata_policy: str = "preserve",
) -> StagedFile:
    if not isinstance(staging_file_url, str) or not staging_file_url:
        raise FileReferenceValidationError("File reference is missing staging_file_url.")
    parsed = urlparse(staging_file_url)
    if parsed.scheme not in {"", "file"} or parsed.netloc not in {"", "localhost"}:
        raise FileReferenceValidationError("File reference must use a local file staging URL.")
    local_path = Path(unquote(parsed.path if parsed.scheme else staging_file_url))
    if not local_path.is_file():
        raise FileReferenceValidationError("Referenced Airbyte staging file is unavailable.")
    if not isinstance(relative_path, str) or not relative_path:
        raise FileReferenceValidationError("File reference is missing a relative source path.")
    if relative_path.startswith("/") or "\\" in relative_path or "//" in relative_path:
        raise FileReferenceValidationError("Relative source path is invalid.")
    if any(part in {"", ".", ".."} for part in relative_path.split("/")):
        raise FileReferenceValidationError("Relative source path contains an invalid segment.")
    if isinstance(file_size_bytes, bool) or not isinstance(file_size_bytes, int) or file_size_bytes < 0:
        raise FileReferenceValidationError("File reference is missing a valid file_size_bytes.")
    actual_size = local_path.stat().st_size
    if actual_size != file_size_bytes:
        raise FileReferenceValidationError("Staged file size does not match file_size_bytes.")
    normalized_hash = sha256 if isinstance(sha256, str) and len(sha256) == 64 else None
    parsed_client_modified = _parse_client_modified(client_modified)
    return StagedFile(
        path=local_path,
        destination_path=f"{root_path}/{relative_path}" if root_path else f"/{relative_path}",
        size=actual_size,
        sha256=normalized_hash,
        client_modified=parsed_client_modified if metadata_policy == "preserve" else None,
    )


def validate_propagation_operation(value: object) -> PropagationOperation:
    if not isinstance(value, dict):
        raise FileReferenceValidationError("Propagation operation must be an object.")
    kind = value.get("operation")
    if kind not in {"move", "delete"}:
        raise FileReferenceValidationError("Propagation operation is invalid.")
    file_id = _required_string(value, "file_id")
    old_path = _validate_relative_path(_required_string(value, "old_path"))
    old_content_hash = _required_string(value, "old_content_hash")
    if kind == "delete":
        return PropagationOperation(kind, file_id, old_path, old_content_hash)
    new_path = _validate_relative_path(_required_string(value, "new_path"))
    new_content_hash = _required_string(value, "new_content_hash")
    return PropagationOperation(
        kind, file_id, old_path, old_content_hash, new_path, new_content_hash
    )


def verify_sha256(file: StagedFile) -> None:
    if not file.sha256:
        return
    digest = hashlib.sha256()
    with file.path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != file.sha256:
        raise FileReferenceValidationError("Staged file SHA-256 does not match source metadata.")


def _required_string(value: dict[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise FileReferenceValidationError(f"Propagation operation requires {key}.")
    return item


def _validate_relative_path(path: str) -> str:
    if path.startswith("/") or "\\" in path or "//" in path:
        raise FileReferenceValidationError("Propagation path is invalid.")
    if any(part in {"", ".", ".."} for part in path.split("/")):
        raise FileReferenceValidationError("Propagation path contains an invalid segment.")
    return path


def _parse_client_modified(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise FileReferenceValidationError("client_modified must be an RFC 3339 timestamp string.")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise FileReferenceValidationError("client_modified must be a valid RFC 3339 timestamp.") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FileReferenceValidationError("client_modified must include a timezone.")
    return parsed.astimezone(UTC)
