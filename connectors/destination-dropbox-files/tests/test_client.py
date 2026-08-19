from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from destination_dropbox_files.client import DropboxFilesClient
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
