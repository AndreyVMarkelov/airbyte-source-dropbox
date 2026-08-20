from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dropbox.exceptions import ApiError

from dropbox_repair.client import DropboxRepairClient, RepairError


def _client() -> DropboxRepairClient:
    client = DropboxRepairClient.__new__(DropboxRepairClient)
    client._client = Mock()
    client._folders = set()
    client._sleeper = Mock()
    return client


def test_staged_file_uses_start_append_finish_with_offsets(tmp_path: Path) -> None:
    chunk = 4
    staged = tmp_path / "staged.bin"
    staged.write_bytes(b"abcdefghijkl")
    client = _client()
    client._client.files_upload_session_start.return_value = SimpleNamespace(session_id="session")

    client.upload_staged_file(staged, "/target.bin", "", chunk)

    client._client.files_upload_session_start.assert_called_once_with(b"abcd")
    append = client._client.files_upload_session_append_v2.call_args
    assert append.args[0] == b"efgh"
    assert append.args[1].offset == 4
    finish = client._client.files_upload_session_finish.call_args
    assert finish.args[0] == b"ijkl"
    assert finish.args[1].offset == 8
    assert finish.args[2].mode.is_overwrite()


def test_exact_chunk_boundary_finishes_with_empty_payload(tmp_path: Path) -> None:
    staged = tmp_path / "staged.bin"
    staged.write_bytes(b"abcd")
    client = _client()
    client._client.files_upload_session_start.return_value = SimpleNamespace(session_id="session")

    client.upload_staged_file(staged, "/target.bin", "", 4)

    finish = client._client.files_upload_session_finish.call_args
    assert finish.args[0] == b""
    assert finish.args[1].offset == 4


def test_append_incorrect_offset_reseeks_before_commit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    staged = tmp_path / "staged.bin"
    staged.write_bytes(b"abcdefghijkl")
    client = _client()
    client._client.files_upload_session_start.return_value = SimpleNamespace(session_id="session")
    error = ApiError("request", Mock(), None, None)
    original = client._call
    failed = False

    def call(action, operation, **kwargs):
        nonlocal failed
        if operation == "upload session append" and not failed:
            failed = True
            raise error
        return original(action, operation, **kwargs)

    monkeypatch.setattr(client, "_call", call)
    monkeypatch.setattr(client, "_correct_offset", Mock(return_value=8))
    client.upload_staged_file(staged, "/target.bin", "", 4)

    finish = client._client.files_upload_session_finish.call_args
    assert finish.args[0] == b"ijkl"
    assert finish.args[1].offset == 8


def test_third_incorrect_offset_recovery_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    staged = tmp_path / "staged.bin"
    staged.write_bytes(b"abcdefghijklmnop")
    client = _client()
    client._client.files_upload_session_start.return_value = SimpleNamespace(session_id="session")
    error = ApiError("request", Mock(), None, None)
    original = client._call

    def call(action, operation, **kwargs):
        if operation == "upload session append":
            raise error
        return original(action, operation, **kwargs)

    monkeypatch.setattr(client, "_call", call)
    monkeypatch.setattr(client, "_correct_offset", Mock(side_effect=[8, 4, 8]))
    with pytest.raises(RepairError, match="unusable offset"):
        client.upload_staged_file(staged, "/target.bin", "", 4)
