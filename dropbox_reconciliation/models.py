from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

Status = Literal["matched", "missing", "mismatched", "extra_destination", "error"]


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
            "client_modified": self.client_modified.isoformat() if self.client_modified else None,
            "server_modified": self.server_modified.isoformat() if self.server_modified else None,
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
            "path": self.path,
            "status": self.status,
            "reason": self.reason,
            "source": self.source.report_metadata() if self.source else None,
            "destination": self.destination.report_metadata() if self.destination else None,
        }
