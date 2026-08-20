from datetime import UTC, datetime

from dropbox_reconciliation.models import FileInventoryItem, Inventory, InventoryPathIssue
from dropbox_reconciliation.reconcile import reconcile, summarize


def _item(path: str, *, size: int = 10, content_hash: str | None = "hash") -> FileInventoryItem:
    return FileInventoryItem(
        normalized_path=path.lower(),
        display_path=path,
        file_id=f"id:{path}",
        rev="rev",
        size=size,
        content_hash=content_hash,
        client_modified=datetime(2026, 1, 1, tzinfo=UTC),
        server_modified=datetime(2026, 1, 2, tzinfo=UTC),
    )


def test_reconcile_reports_all_statuses_in_normalized_path_order() -> None:
    source = Inventory(
        files={
            "a": _item("A"),
            "b": _item("b", size=11),
            "c": _item("c", content_hash="source"),
            "d": _item("d", content_hash=None),
            "e": _item("e"),
        },
        issues=[InventoryPathIssue("!invalid", "bad/path", "source", _item("bad/path"))],
    )
    destination = Inventory(
        files={
            "a": _item("a"),
            "b": _item("b"),
            "c": _item("c", content_hash="destination"),
            "d": _item("d"),
            "f": _item("f"),
        },
        issues=[],
    )

    records = reconcile(source, destination)

    assert [(record.path, record.status, record.reason) for record in records] == [
        ("bad/path", "error", "invalid_relative_path"),
        ("A", "matched", "matched"),
        ("b", "mismatched", "size_mismatch"),
        ("c", "mismatched", "content_hash_mismatch"),
        ("d", "error", "missing_content_hash"),
        ("e", "missing", "source_only"),
        ("f", "extra_destination", "destination_only"),
    ]
    assert summarize(records) == {
        "type": "summary",
        "matched": 1,
        "missing": 1,
        "mismatched": 2,
        "extra_destination": 1,
        "errors": 2,
    }
    assert records[1].as_dict()["source"]["file_id"] == "id:A"
    assert records[1].as_dict()["source"]["client_modified"] == "2026-01-01T00:00:00+00:00"
