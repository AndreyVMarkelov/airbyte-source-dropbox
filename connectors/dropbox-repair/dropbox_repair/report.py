from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ACTIONABLE = frozenset({"missing", "mismatched"})
KNOWN_STATUSES = frozenset({"matched", "missing", "mismatched", "extra_destination", "error"})


class RepairReportError(ValueError):
    pass


@dataclass(frozen=True)
class RepairRecord:
    path: str
    status: str
    source: dict[str, Any] | None


def load_report(path: str) -> list[RepairRecord]:
    try:
        lines = [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise RepairReportError("Report must be valid JSONL.") from exc
    if not lines or not isinstance(lines[-1], dict) or lines[-1].get("type") != "summary":
        raise RepairReportError("Report must end with a summary record.")
    records: list[RepairRecord] = []
    seen_paths: set[str] = set()
    for value in lines[:-1]:
        if not isinstance(value, dict):
            raise RepairReportError("Report records must be objects.")
        path_value, status = value.get("path"), value.get("status")
        if not isinstance(path_value, str) or not _safe_relative_path(path_value):
            raise RepairReportError("Report contains an invalid relative path.")
        if status not in KNOWN_STATUSES:
            raise RepairReportError("Report contains an unknown status.")
        normalized = path_value.casefold()
        if normalized in seen_paths:
            raise RepairReportError("Report contains duplicate paths.")
        seen_paths.add(normalized)
        source = value.get("source")
        if source is not None and not isinstance(source, dict):
            raise RepairReportError("Report source metadata must be an object or null.")
        if status in ACTIONABLE:
            _validate_actionable_source(source)
        records.append(RepairRecord(path_value, status, source))
    return records


def _validate_actionable_source(source: dict[str, Any] | None) -> None:
    if source is None:
        raise RepairReportError("Actionable report record is missing source metadata.")
    for field, expected in (("file_id", str), ("rev", str), ("content_hash", str), ("size", int)):
        value = source.get(field)
        if (
            isinstance(value, bool)
            or not isinstance(value, expected)
            or (expected is str and not value)
        ):
            raise RepairReportError(f"Actionable report source metadata has an invalid {field}.")
    if source["size"] < 0:
        raise RepairReportError("Actionable report source metadata has an invalid size.")


def _safe_relative_path(path: str) -> bool:
    return (
        not path.startswith("/")
        and "\\" not in path
        and "//" not in path
        and all(part not in {"", ".", ".."} for part in path.split("/"))
    )
