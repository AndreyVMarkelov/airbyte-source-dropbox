from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteRecordMessageFileReference,
    Type,
)

from destination_dropbox_files.client import DropboxFilesWriteError
from destination_dropbox_files.destination import DestinationDropboxFiles


def _config() -> dict[str, object]:
    return {
        "credentials": {"auth_type": "access_token", "access_token": "token"},
        "root_path": "",
    }


def _catalog() -> object:
    return SimpleNamespace(streams=[SimpleNamespace(stream=SimpleNamespace(name="raw_files"))])


def _record(path: str) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(
            stream="raw_files",
            data={},
            emitted_at=0,
            file_reference=AirbyteRecordMessageFileReference(
                staging_file_url=path,
                source_file_relative_path="file.bin",
                file_size_bytes=1,
            ),
        ),
    )


def test_successful_reference_releases_following_state(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    staged = tmp_path / "file.bin"
    staged.write_bytes(b"x")
    client = Mock()
    monkeypatch.setattr("destination_dropbox_files.destination.DropboxFilesClient", Mock(return_value=client))
    state = AirbyteMessage(type=Type.STATE)

    output = list(DestinationDropboxFiles().write(_config(), _catalog(), [_record(staged.as_uri()), state]))

    client.upload_staged_file.assert_called_once()
    assert output == [state]


def test_failed_reference_withholds_following_state(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    staged = tmp_path / "file.bin"
    staged.write_bytes(b"x")
    client = Mock()
    client.upload_staged_file.side_effect = DropboxFilesWriteError("upload failed")
    monkeypatch.setattr("destination_dropbox_files.destination.DropboxFilesClient", Mock(return_value=client))

    with pytest.raises(DropboxFilesWriteError):
        list(DestinationDropboxFiles().write(_config(), _catalog(), [_record(staged.as_uri()), AirbyteMessage(type=Type.STATE)]))
