from __future__ import annotations

from dropbox_reconciliation.models import (
    FileInventoryItem,
    Inventory,
    ReconciliationRecord,
    client_modified_dimension,
)


def reconcile(source: Inventory, destination: Inventory) -> list[ReconciliationRecord]:
    records = [
        _issue_record(issue.path, issue.sort_key, issue.side, issue.item) for issue in source.issues
    ]
    records.extend(
        _issue_record(issue.path, issue.sort_key, issue.side, issue.item)
        for issue in destination.issues
    )
    for path in sorted(source.files.keys() | destination.files.keys()):
        source_item = source.files.get(path)
        destination_item = destination.files.get(path)
        records.append(_compare_path(path, source_item, destination_item))
    return sorted(records, key=lambda record: record.sort_key)


def summarize(records: list[ReconciliationRecord]) -> dict[str, object]:
    counts = {"matched": 0, "missing": 0, "mismatched": 0, "extra_destination": 0, "errors": 0}
    metadata_mismatches = {"client_modified": 0}
    for record in records:
        if record.status == "error":
            counts["errors"] += 1
        else:
            counts[record.status] += 1
        if client_modified_dimension(record.source, record.destination)["status"] == "mismatched":
            metadata_mismatches["client_modified"] += 1
    return {
        "type": "summary",
        "total_paths": len(records),
        **counts,
        "metadata_mismatches": metadata_mismatches,
    }


def _issue_record(
    path: str, sort_key: str, side: str, item: FileInventoryItem
) -> ReconciliationRecord:
    return ReconciliationRecord(
        sort_key=sort_key,
        path=path,
        status="error",
        reason="invalid_relative_path",
        source=item if side == "source" else None,
        destination=item if side == "destination" else None,
    )


def _compare_path(
    path: str, source: FileInventoryItem | None, destination: FileInventoryItem | None
) -> ReconciliationRecord:
    display_path = source.display_path if source else destination.display_path  # type: ignore[union-attr]
    if source is None:
        return ReconciliationRecord(
            path, display_path, "extra_destination", "destination_only", None, destination
        )
    if destination is None:
        return ReconciliationRecord(path, display_path, "missing", "source_only", source, None)
    if source.content_hash is None or destination.content_hash is None:
        return ReconciliationRecord(
            path, display_path, "error", "missing_content_hash", source, destination
        )
    if source.size != destination.size:
        return ReconciliationRecord(
            path, display_path, "mismatched", "size_mismatch", source, destination
        )
    if source.content_hash != destination.content_hash:
        return ReconciliationRecord(
            path, display_path, "mismatched", "content_hash_mismatch", source, destination
        )
    return ReconciliationRecord(path, display_path, "matched", "matched", source, destination)
