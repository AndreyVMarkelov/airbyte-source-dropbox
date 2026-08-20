from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dropbox.exceptions import InternalServerError

from dropbox_repair.cli import _apply
from dropbox_repair.client import DropboxRepairClient, RepairError
from dropbox_repair.report import RepairRecord


def test_apply_emits_only_after_upload_and_skips_non_actionable(tmp_path: Path, capsys) -> None:
    staged = tmp_path / "staged"
    staged.write_bytes(b"content")
    source = SimpleNamespace(stage_source_file=lambda *_args: staged)
    uploads: list[str] = []
    destination = SimpleNamespace(
        upload_staged_file=lambda _staged, path, *_args: uploads.append(path)
    )
    records = [
        RepairRecord(
            "new.bin", "missing", {"file_id": "id", "rev": "r", "size": 7, "content_hash": "h"}
        ),
        RepairRecord("same.bin", "matched", None),
        RepairRecord(
            "old.bin", "mismatched", {"file_id": "id", "rev": "r", "size": 7, "content_hash": "h"}
        ),
    ]

    _apply(records, source, destination, "/source", "/destination", 1024)

    assert uploads == ["/destination/new.bin", "/destination/old.bin"]
    assert not staged.exists()
    assert '"action": "uploaded"' in capsys.readouterr().out


def test_apply_stops_on_destination_failure(tmp_path: Path) -> None:
    staged = tmp_path / "staged"
    staged.write_bytes(b"content")
    source = SimpleNamespace(stage_source_file=lambda *_args: staged)
    destination = SimpleNamespace(
        upload_staged_file=lambda *_args: (_ for _ in ()).throw(RuntimeError("failed"))
    )
    records = [
        RepairRecord(
            "new.bin", "missing", {"file_id": "id", "rev": "r", "size": 7, "content_hash": "h"}
        )
    ]

    with pytest.raises(RuntimeError, match="failed"):
        _apply(records, source, destination, "/source", "/destination", 1024)
    assert not staged.exists()


def test_transient_retries_are_bounded() -> None:
    client = DropboxRepairClient.__new__(DropboxRepairClient)
    client._sleeper = Mock()
    action = Mock(side_effect=[InternalServerError("request", 500, None), "ok"])

    assert client._call(action, "upload") == "ok"
    client._sleeper.assert_called_once_with(1)

    action.side_effect = InternalServerError("request", 500, None)
    with pytest.raises(RepairError, match="retry budget"):
        client._call(action, "upload")
