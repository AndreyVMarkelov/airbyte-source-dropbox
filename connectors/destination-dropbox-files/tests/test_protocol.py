from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteRecordMessageFileReference,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    StreamDescriptor,
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
            data={"client_modified": "2026-08-18T14:00:00+02:00"},
            emitted_at=0,
            file_reference=AirbyteRecordMessageFileReference(
                staging_file_url=path,
                source_file_relative_path="file.bin",
                file_size_bytes=1,
            ),
        ),
    )


def _state(version: int) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.STATE,
        state=AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="raw_files"),
                stream_state={"version": version},
            ),
        ),
    )


def _operation() -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(
            stream="raw_files",
            data={
                "operation": "delete",
                "file_id": "id:file",
                "old_path": "gone.pdf",
                "old_content_hash": "hash",
            },
            emitted_at=0,
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
    assert client.upload_staged_file.call_args.args[0].client_modified is not None
    assert output == [state]


def test_failed_reference_withholds_following_state(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    staged = tmp_path / "file.bin"
    staged.write_bytes(b"x")
    client = Mock()
    client.upload_staged_file.side_effect = DropboxFilesWriteError("upload failed")
    monkeypatch.setattr("destination_dropbox_files.destination.DropboxFilesClient", Mock(return_value=client))

    with pytest.raises(DropboxFilesWriteError):
        list(DestinationDropboxFiles().write(_config(), _catalog(), [_record(staged.as_uri()), AirbyteMessage(type=Type.STATE)]))


def test_retry_forwards_updated_file_state_only_after_successful_upload(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    staged = tmp_path / "file.bin"
    staged.write_bytes(b"x")
    state_v2 = _state(2)
    failed_client = Mock()
    failed_client.upload_staged_file.side_effect = DropboxFilesWriteError("upload failed")
    monkeypatch.setattr(
        "destination_dropbox_files.destination.DropboxFilesClient", Mock(return_value=failed_client)
    )

    with pytest.raises(DropboxFilesWriteError):
        list(DestinationDropboxFiles().write(_config(), _catalog(), [_record(staged.as_uri()), state_v2]))

    succeeding_client = Mock()
    monkeypatch.setattr(
        "destination_dropbox_files.destination.DropboxFilesClient", Mock(return_value=succeeding_client)
    )
    assert list(DestinationDropboxFiles().write(_config(), _catalog(), [_record(staged.as_uri()), state_v2])) == [state_v2]


def test_propagation_operation_forwards_state_only_after_success(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    state = _state(2)
    failed_client = Mock()
    failed_client.apply_propagation.side_effect = DropboxFilesWriteError("delete failed")
    monkeypatch.setattr(
        "destination_dropbox_files.destination.DropboxFilesClient", Mock(return_value=failed_client)
    )

    with pytest.raises(DropboxFilesWriteError):
        list(DestinationDropboxFiles().write(_config(), _catalog(), [_operation(), state]))

    succeeding_client = Mock()
    monkeypatch.setattr(
        "destination_dropbox_files.destination.DropboxFilesClient", Mock(return_value=succeeding_client)
    )
    assert list(DestinationDropboxFiles().write(_config(), _catalog(), [_operation(), state])) == [state]
