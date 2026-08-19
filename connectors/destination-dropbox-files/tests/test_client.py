from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dropbox.exceptions import InternalServerError, RateLimitError

from destination_dropbox_files.client import DropboxFilesClient, DropboxFilesWriteError
from destination_dropbox_files.validation import StagedFile


def _client() -> DropboxFilesClient:
    client = DropboxFilesClient(
        {
            "credentials": {"auth_type": "access_token", "access_token": "token"},
            "upload_chunk_size_mb": 1,
        },
        sleeper=Mock(),
    )
    client._client = Mock()
    return client


def test_streams_staged_file_in_session_chunks_and_commits(tmp_path: Path) -> None:
    content = b"a" * (1024 * 1024) + b"b" * 3
    path = tmp_path / "staged.bin"
    path.write_bytes(content)
    staged = StagedFile(path=path, destination_path="/root/file.bin", size=len(content), sha256=None)
    client = _client()
    client._client.files_upload_session_start.return_value = SimpleNamespace(session_id="session")

    client.upload_staged_file(staged, "", "overwrite")

    client._client.files_upload_session_start.assert_called_once_with(b"a" * (1024 * 1024))
    client._client.files_upload_session_append_v2.assert_not_called()
    finish = client._client.files_upload_session_finish.call_args
    assert finish.args[0] == b"b" * 3
    assert finish.args[1].offset == 1024 * 1024
    assert finish.args[2].path == "/root/file.bin"


def test_creates_only_children_below_configured_root(tmp_path: Path) -> None:
    path = tmp_path / "staged.bin"
    path.write_bytes(b"x")
    client = _client()
    client._client.files_upload_session_start.return_value = SimpleNamespace(session_id="session")
    client.upload_staged_file(
        StagedFile(path=path, destination_path="/Exports/child/file.bin", size=1, sha256=None),
        "/Exports",
        "overwrite",
    )

    client._client.files_create_folder_v2.assert_called_once_with("/Exports/child", autorename=False)


def test_multi_chunk_upload_appends_before_final_commit(tmp_path: Path) -> None:
    chunk = 1024 * 1024
    path = tmp_path / "staged.bin"
    path.write_bytes(b"a" * chunk + b"b" * chunk + b"c")
    client = _client()
    client._client.files_upload_session_start.return_value = SimpleNamespace(session_id="session")

    client.upload_staged_file(
        StagedFile(path=path, destination_path="/file.bin", size=chunk * 2 + 1, sha256=None),
        "",
        "fail",
    )

    append = client._client.files_upload_session_append_v2.call_args
    assert append.args[0] == b"b" * chunk
    assert append.args[1].offset == chunk
    finish = client._client.files_upload_session_finish.call_args
    assert finish.args[0] == b"c"
    assert finish.args[1].offset == chunk * 2
    assert finish.args[2].mode.is_add()
    assert finish.args[2].strict_conflict is True


def test_exact_chunk_boundary_finishes_with_empty_payload(tmp_path: Path) -> None:
    chunk = 1024 * 1024
    path = tmp_path / "staged.bin"
    path.write_bytes(b"a" * chunk)
    client = _client()
    client._client.files_upload_session_start.return_value = SimpleNamespace(session_id="session")

    client.upload_staged_file(
        StagedFile(path=path, destination_path="/file.bin", size=chunk, sha256=None), "", "overwrite"
    )

    finish = client._client.files_upload_session_finish.call_args
    assert finish.args[0] == b""
    assert finish.args[1].offset == chunk
    assert finish.args[2].mode.is_overwrite()


def test_transient_request_retries_then_succeeds() -> None:
    sleeper = Mock()
    client = DropboxFilesClient(
        {"credentials": {"auth_type": "access_token", "access_token": "token"}}, sleeper=sleeper
    )
    action = Mock(side_effect=[RateLimitError("request", backoff=0), "ok"])

    assert client._call("append", action) == "ok"
    assert action.call_count == 2
    sleeper.assert_called_once_with(1)


def test_transient_request_exhaustion_is_a_write_error() -> None:
    client = _client()
    with pytest.raises(DropboxFilesWriteError, match="retry budget"):
        client._call("append", Mock(side_effect=InternalServerError("request", 500, "error")))
