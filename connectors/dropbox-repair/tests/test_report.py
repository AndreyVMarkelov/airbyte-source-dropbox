import json

import pytest

from dropbox_repair.report import RepairReportError, load_report


def _actionable(path: str, status: str = "missing") -> dict[str, object]:
    return {
        "path": path,
        "status": status,
        "source": {"file_id": "id:1", "rev": "rev", "size": 1, "content_hash": "hash"},
    }


def test_report_accepts_actionable_and_skipped_records(tmp_path) -> None:
    report = tmp_path / "report.jsonl"
    report.write_text(
        "\n".join(
            map(
                json.dumps,
                [
                    _actionable("nested/a.bin"),
                    {"path": "extra.bin", "status": "extra_destination", "source": None},
                    {"type": "summary"},
                ],
            )
        )
    )

    records = load_report(str(report))

    assert [(record.path, record.status) for record in records] == [
        ("nested/a.bin", "missing"),
        ("extra.bin", "extra_destination"),
    ]


@pytest.mark.parametrize(
    "records",
    [
        [_actionable("../escape")],
        [_actionable("same"), _actionable("SAME")],
        [{"path": "a", "status": "unknown", "source": None}],
        [{"path": "a", "status": "missing", "source": None}],
    ],
)
def test_report_fails_closed_for_invalid_records(tmp_path, records) -> None:
    report = tmp_path / "report.jsonl"
    report.write_text("\n".join(map(json.dumps, [*records, {"type": "summary"}])))

    with pytest.raises(RepairReportError):
        load_report(str(report))


def test_report_requires_summary_last(tmp_path) -> None:
    report = tmp_path / "report.jsonl"
    report.write_text(json.dumps(_actionable("a")))

    with pytest.raises(RepairReportError, match="summary"):
        load_report(str(report))
