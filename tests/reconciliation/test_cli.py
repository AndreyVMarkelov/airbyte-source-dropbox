import json
import sys
from pathlib import Path

import pytest

from dropbox_reconciliation.cli import main
from dropbox_reconciliation.models import FileInventoryItem, Inventory


class FakeClient:
    def __init__(self, _config, side: str) -> None:
        self.side = side

    def validate_root(self, root: str) -> str:
        return root

    def inventory(self, _root: str) -> Inventory:
        item = FileInventoryItem("file.txt", "file.txt", "id:1", "rev", 1, "hash", None, None)
        return Inventory({"file.txt": item} if self.side == "source" else {}, [])


def _config(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {
                "source": {"credentials": {}, "root_path": ""},
                "destination": {"credentials": {}, "root_path": ""},
            }
        )
    )
    return path


def test_cli_writes_jsonl_to_output_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr("dropbox_reconciliation.cli.DropboxReconciliationClient", FakeClient)
    config = _config(tmp_path / "config.json")
    output = tmp_path / "report.jsonl"
    monkeypatch.setattr(
        sys,
        "argv",
        ["dropbox-reconciliation", "compare", "--config", str(config), "--output", str(output)],
    )

    main()

    lines = [json.loads(line) for line in output.read_text().splitlines()]
    assert lines[0]["reason"] == "source_only"
    assert lines[-1] == {
        "type": "summary",
        "total_paths": 1,
        "matched": 0,
        "missing": 1,
        "mismatched": 0,
        "extra_destination": 0,
        "errors": 0,
        "metadata_mismatches": {"client_modified": 0},
    }


def test_cli_writes_jsonl_to_stdout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("dropbox_reconciliation.cli.DropboxReconciliationClient", FakeClient)
    config = _config(tmp_path / "config.json")
    monkeypatch.setattr(sys, "argv", ["dropbox-reconciliation", "compare", "--config", str(config)])

    main()

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert lines[0]["status"] == "missing"
    assert lines[-1]["type"] == "summary"


def test_cli_returns_nonzero_for_invalid_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = tmp_path / "bad.json"
    config.write_text("[]")
    monkeypatch.setattr(sys, "argv", ["dropbox-reconciliation", "compare", "--config", str(config)])

    with pytest.raises(SystemExit, match="1"):
        main()
