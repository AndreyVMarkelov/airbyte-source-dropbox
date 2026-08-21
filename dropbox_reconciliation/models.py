from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

Status = Literal["matched", "missing", "mismatched", "extra_destination", "error"]
MetadataStatus = Literal[
    "matched", "mismatched", "source_missing", "destination_missing", "not_comparable"
]


@dataclass(frozen=True)
class FileInventoryItem:
    normalized_path: str
    display_path: str
    file_id: str | None
    rev: str | None
    size: int | None
    content_hash: str | None
    client_modified: datetime | None
    server_modified: datetime | None

    def report_metadata(self) -> dict[str, Any]:
        return {
            "file_id": self.file_id,
            "rev": self.rev,
            "size": self.size,
            "content_hash": self.content_hash,
            "client_modified": format_datetime_utc(self.client_modified),
            "server_modified": format_datetime_utc(self.server_modified),
        }


@dataclass(frozen=True)
class InventoryPathIssue:
    sort_key: str
    path: str
    side: Literal["source", "destination"]
    item: FileInventoryItem


@dataclass(frozen=True)
class Inventory:
    files: dict[str, FileInventoryItem]
    issues: list[InventoryPathIssue]


@dataclass(frozen=True)
class ReconciliationRecord:
    sort_key: str
    path: str
    status: Status
    reason: str
    source: FileInventoryItem | None
    destination: FileInventoryItem | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "type": "file",
            "path": self.path,
            "status": self.status,
            "reason": self.reason,
            "content": content_dimension(self.status, self.reason),
            "namespace": namespace_dimension(self.status),
            "metadata": metadata_dimension(self.source, self.destination),
            "source": self.source.report_metadata() if self.source else None,
            "destination": self.destination.report_metadata() if self.destination else None,
        }


def content_dimension(status: Status, reason: str) -> dict[str, str]:
    if status == "matched":
        return {"status": "matched", "reason": "content_match"}
    return {"status": status, "reason": reason}


def namespace_dimension(status: Status) -> dict[str, str]:
    if status == "matched":
        return {"status": "matched"}
    if status == "missing":
        return {"status": "missing", "reason": "source_only"}
    if status == "extra_destination":
        return {"status": "extra_destination", "reason": "destination_only"}
    return {"status": "not_comparable"}


def metadata_dimension(
    source: FileInventoryItem | None, destination: FileInventoryItem | None
) -> dict[str, dict[str, str]]:
    return {"client_modified": client_modified_dimension(source, destination)}


def client_modified_dimension(
    source: FileInventoryItem | None, destination: FileInventoryItem | None
) -> dict[str, str]:
    if source is None or destination is None:
        return {"status": "not_comparable"}
    source_modified = normalize_datetime_utc(source.client_modified)
    destination_modified = normalize_datetime_utc(destination.client_modified)
    if (
        source_modified is None
        and source.client_modified is None
        and destination.client_modified is None
    ):
        return {"status": "not_comparable"}
    if source_modified is None and source.client_modified is None:
        return {"status": "source_missing"}
    if destination_modified is None and destination.client_modified is None:
        return {"status": "destination_missing"}
    if source_modified is None or destination_modified is None:
        return {"status": "not_comparable"}
    if source_modified == destination_modified:
        return {"status": "matched"}
    return {
        "status": "mismatched",
        "source": format_datetime_utc(source_modified),
        "destination": format_datetime_utc(destination_modified),
    }


def normalize_datetime_utc(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def format_datetime_utc(value: object) -> str | None:
    normalized = normalize_datetime_utc(value)
    if normalized is None:
        return None
    return normalized.isoformat().replace("+00:00", "Z")
