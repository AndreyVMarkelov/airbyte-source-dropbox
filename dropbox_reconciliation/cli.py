from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, TextIO

from dropbox_reconciliation.client import DropboxReconciliationClient, ReconciliationError
from dropbox_reconciliation.reconcile import reconcile, summarize


def main() -> None:
    parser = argparse.ArgumentParser(prog="dropbox-reconciliation")
    commands = parser.add_subparsers(dest="command", required=True)
    compare = commands.add_parser("compare", help="compare two Dropbox folder roots using metadata")
    compare.add_argument("--config", required=True, help="path to reconciliation JSON config")
    compare.add_argument("--output", help="optional JSONL report path; defaults to stdout")
    arguments = parser.parse_args()
    try:
        config = _load_config(arguments.config)
        source_config = _side_config(config, "source")
        destination_config = _side_config(config, "destination")
        source_client = DropboxReconciliationClient(source_config, "source")
        destination_client = DropboxReconciliationClient(destination_config, "destination")
        source_root = source_client.validate_root(source_config.get("root_path"))
        destination_root = destination_client.validate_root(destination_config.get("root_path"))
        records = reconcile(
            source_client.inventory(source_root), destination_client.inventory(destination_root)
        )
        _write_report(records, arguments.output)
    except (OSError, ValueError, ReconciliationError) as exc:
        print(f"Reconciliation failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


def _load_config(path: str) -> Mapping[str, Any]:
    value = json.loads(Path(path).read_text())
    if not isinstance(value, Mapping):
        raise ReconciliationError("reconciliation config must be a JSON object.")
    return value


def _side_config(config: Mapping[str, Any], side: str) -> Mapping[str, Any]:
    value = config.get(side)
    if not isinstance(value, Mapping):
        raise ReconciliationError(f"{side} configuration is required.")
    return value


def _write_report(records: list[Any], output: str | None) -> None:
    stream: TextIO
    if output:
        stream = Path(output).open("w", encoding="utf-8")
    else:
        stream = sys.stdout
    try:
        for record in records:
            print(json.dumps(record.as_dict(), sort_keys=True), file=stream)
        print(json.dumps(summarize(records), sort_keys=True), file=stream)
    finally:
        if output:
            stream.close()
