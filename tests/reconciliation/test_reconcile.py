from datetime import UTC, datetime, timedelta, timezone

from dropbox_reconciliation.models import FileInventoryItem, Inventory, InventoryPathIssue
from dropbox_reconciliation.reconcile import reconcile, summarize


def _item(
    path: str,
    *,
    size: int = 10,
    content_hash: str | None = "hash",
    client_modified: object = datetime(2026, 1, 1, tzinfo=UTC),
    server_modified: object = datetime(2026, 1, 2, tzinfo=UTC),
) -> FileInventoryItem:
    return FileInventoryItem(
        normalized_path=path.lower(),
        display_path=path,
        file_id=f"id:{path}",
        rev="rev",
        size=size,
        content_hash=content_hash,
        client_modified=client_modified,  # type: ignore[arg-type]
        server_modified=server_modified,  # type: ignore[arg-type]
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
        "total_paths": 7,
        "matched": 1,
        "missing": 1,
        "mismatched": 2,
        "extra_destination": 1,
        "errors": 2,
        "metadata_mismatches": {"client_modified": 0},
    }
    record = records[1].as_dict()
    assert record["type"] == "file"
    assert record["content"] == {"status": "matched", "reason": "content_match"}
    assert record["namespace"] == {"status": "matched"}
    assert record["metadata"] == {"client_modified": {"status": "matched"}}
    assert record["source"]["file_id"] == "id:A"
    assert record["source"]["client_modified"] == "2026-01-01T00:00:00Z"


def test_reconcile_reports_client_modified_mismatch_without_changing_content_status() -> None:
    source = Inventory(
        files={
            "report.pdf": _item(
                "report.pdf", client_modified=datetime(2026, 8, 20, 10, tzinfo=UTC)
            )
        },
        issues=[],
    )
    destination = Inventory(
        files={
            "report.pdf": _item(
                "report.pdf", client_modified=datetime(2026, 8, 20, 11, tzinfo=UTC)
            )
        },
        issues=[],
    )

    records = reconcile(source, destination)

    assert [(record.status, record.reason) for record in records] == [("matched", "matched")]
    assert records[0].as_dict()["content"] == {"status": "matched", "reason": "content_match"}
    assert records[0].as_dict()["metadata"] == {
        "client_modified": {
            "status": "mismatched",
            "source": "2026-08-20T10:00:00Z",
            "destination": "2026-08-20T11:00:00Z",
        }
    }
    assert summarize(records)["metadata_mismatches"] == {"client_modified": 1}


def test_reconcile_compares_client_modified_instants_not_string_format() -> None:
    source = Inventory(
        files={
            "report.pdf": _item(
                "report.pdf", client_modified=datetime(2026, 8, 20, 10, tzinfo=UTC)
            )
        },
        issues=[],
    )
    destination = Inventory(
        files={
            "report.pdf": _item(
                "report.pdf",
                client_modified=datetime(
                    2026, 8, 20, 12, tzinfo=timezone(timedelta(hours=2))
                ),
            )
        },
        issues=[],
    )

    record = reconcile(source, destination)[0].as_dict()

    assert record["status"] == "matched"
    assert record["metadata"] == {"client_modified": {"status": "matched"}}


def test_reconcile_metadata_missing_and_malformed_do_not_change_content_status() -> None:
    source_missing = reconcile(
        Inventory({"report.pdf": _item("report.pdf", client_modified=None)}, []),
        Inventory({"report.pdf": _item("report.pdf")}, []),
    )[0].as_dict()
    destination_missing = reconcile(
        Inventory({"report.pdf": _item("report.pdf")}, []),
        Inventory({"report.pdf": _item("report.pdf", client_modified=None)}, []),
    )[0].as_dict()
    malformed = reconcile(
        Inventory({"report.pdf": _item("report.pdf", client_modified="not-a-date")}, []),
        Inventory({"report.pdf": _item("report.pdf")}, []),
    )[0].as_dict()

    assert source_missing["status"] == "matched"
    assert source_missing["metadata"] == {"client_modified": {"status": "source_missing"}}
    assert destination_missing["metadata"] == {
        "client_modified": {"status": "destination_missing"}
    }
    assert malformed["metadata"] == {"client_modified": {"status": "not_comparable"}}


def test_reconcile_ignores_server_modified_for_fidelity() -> None:
    source = Inventory(
        files={
            "report.pdf": _item(
                "report.pdf", server_modified=datetime(2026, 8, 20, tzinfo=UTC)
            )
        },
        issues=[],
    )
    destination = Inventory(
        files={
            "report.pdf": _item(
                "report.pdf", server_modified=datetime(2026, 8, 21, tzinfo=UTC)
            )
        },
        issues=[],
    )

    record = reconcile(source, destination)[0].as_dict()

    assert record["status"] == "matched"
    assert record["metadata"] == {"client_modified": {"status": "matched"}}
