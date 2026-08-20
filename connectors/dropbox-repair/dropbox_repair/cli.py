from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dropbox_repair.client import (
    DropboxRepairClient,
    RepairError,
    SourceDriftError,
    destination_path,
)
from dropbox_repair.report import ACTIONABLE, RepairRecord, load_report


def main() -> None:
    parser = argparse.ArgumentParser(prog="dropbox-repair")
    commands = parser.add_subparsers(dest="command", required=True)
    apply = commands.add_parser(
        "apply", help="repair actionable files from a reconciliation JSONL report"
    )
    apply.add_argument("--report", required=True)
    apply.add_argument("--source-config", required=True)
    apply.add_argument("--destination-config", required=True)
    apply.add_argument("--chunk-size-mb", type=int, default=8)
    args = parser.parse_args()
    try:
        records = load_report(args.report)
        chunk_size = _chunk_size(args.chunk_size_mb)
        source_config = _load_config(args.source_config)
        destination_config = _load_config(args.destination_config)
        source = DropboxRepairClient(source_config, "source")
        destination = DropboxRepairClient(destination_config, "destination")
        source_root = source.validate_root(source_config.get("root_path"))
        destination_root = destination.validate_root(destination_config.get("root_path"))
        _apply(records, source, destination, source_root, destination_root, chunk_size)
    except (OSError, ValueError, RepairError) as exc:
        print(f"Repair failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


def _apply(
    records: list[RepairRecord],
    source: DropboxRepairClient,
    destination: DropboxRepairClient,
    source_root: str,
    destination_root: str,
    chunk_size: int,
) -> None:
    counts = {"uploaded": 0, "overwritten": 0, "skipped": 0, "errors": 0}
    for record in records:
        if record.status not in ACTIONABLE:
            counts["skipped"] += 1
            _emit(record.path, "skipped", record.status)
            continue
        staged = None
        try:
            staged = source.stage_source_file(
                record.source or {}, source_root, record.path, chunk_size
            )
            destination.upload_staged_file(
                staged,
                destination_path(destination_root, record.path),
                destination_root,
                chunk_size,
            )
        except SourceDriftError:
            counts["errors"] += 1
            _emit(record.path, "error", record.status)
            continue
        finally:
            if staged:
                staged.unlink(missing_ok=True)
        action = "uploaded" if record.status == "missing" else "overwritten"
        counts[f"{action}"] += 1
        _emit(record.path, action, record.status)
    print(json.dumps({"type": "summary", **counts}, sort_keys=True))


def _emit(path: str, action: str, source_status: str) -> None:
    print(
        json.dumps({"path": path, "action": action, "source_status": source_status}, sort_keys=True)
    )


def _load_config(path: str) -> Mapping[str, Any]:
    value = json.loads(Path(path).read_text())
    if not isinstance(value, Mapping):
        raise RepairError("Configuration must be a JSON object.")
    return value


def _chunk_size(value: int) -> int:
    if not 1 <= value <= 16:
        raise RepairError("chunk-size-mb must be between 1 and 16.")
    return value * 1024 * 1024
