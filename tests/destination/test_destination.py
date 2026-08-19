import base64
from unittest.mock import Mock, patch

import pytest
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Status,
    SyncMode,
    Type,
)

from destination_dropbox.client import DropboxWriteError
from destination_dropbox.destination import DestinationDropbox
from destination_dropbox.validation import RecordValidationError

CONFIG = {
    "credentials": {"auth_type": "access_token", "access_token": "test-token"},
    "root_path": "/Exports",
    "max_file_size_mb": 10,
}


def _catalog(*names: str) -> ConfiguredAirbyteCatalog:
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=name, json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            )
            for name in names
        ]
    )


def _record(stream: str, data: dict[str, object]) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(stream=stream, data=data, emitted_at=0),
    )


def _data() -> dict[str, object]:
    return {"path": "folder/report.pdf", "content_base64": base64.b64encode(b"report").decode()}


def test_check_uses_dropbox_account_api() -> None:
    with patch("destination_dropbox.destination.DropboxClient") as client_cls:
        client_cls.return_value.current_account.return_value = Mock()
        result = DestinationDropbox().check(Mock(), CONFIG)

    assert result == AirbyteConnectionStatus(status=Status.SUCCEEDED)


def test_check_returns_clean_failure() -> None:
    with patch("destination_dropbox.destination.DropboxClient") as client_cls:
        client_cls.return_value.current_account.side_effect = RuntimeError("bad credentials")
        result = DestinationDropbox().check(Mock(), CONFIG)

    assert result.status == Status.FAILED
    assert "bad credentials" in result.message


def test_write_uploads_configured_records_in_input_order() -> None:
    messages = [
        _record("documents", _data()),
        _record(
            "documents",
            {
                "path": "folder/second.pdf",
                "content_base64": base64.b64encode(b"second").decode(),
            },
        ),
    ]
    client = Mock()

    with patch("destination_dropbox.destination.DropboxClient", return_value=client):
        assert list(DestinationDropbox().write(CONFIG, _catalog("documents"), messages)) == []

    assert [call.args[0].destination_path for call in client.upload_file.call_args_list] == [
        "/Exports/folder/report.pdf",
        "/Exports/folder/second.pdf",
    ]
    assert [call.args[1] for call in client.upload_file.call_args_list] == [
        "overwrite",
        "overwrite",
    ]


def test_write_emits_state_only_after_preceding_upload() -> None:
    state = AirbyteMessage(type=Type.STATE)
    client = Mock()
    messages = [_record("documents", _data()), state]

    with patch("destination_dropbox.destination.DropboxClient", return_value=client):
        output = list(DestinationDropbox().write(CONFIG, _catalog("documents"), messages))

    assert output == [state]
    client.upload_file.assert_called_once()


def test_write_does_not_emit_later_state_after_upload_failure() -> None:
    first_state = AirbyteMessage(type=Type.STATE)
    later_state = AirbyteMessage(type=Type.STATE)
    client = Mock()
    client.upload_file.side_effect = [None, DropboxWriteError("Dropbox upload failed")]
    messages = [
        _record("documents", _data()),
        first_state,
        _record("documents", _data()),
        later_state,
    ]

    with patch("destination_dropbox.destination.DropboxClient", return_value=client):
        output = DestinationDropbox().write(CONFIG, _catalog("documents"), messages)
        assert next(output) == first_state
        with pytest.raises(DropboxWriteError, match="record 2 from stream 'documents'"):
            next(output)

    assert client.upload_file.call_count == 2


def test_write_rejects_unknown_stream_with_identity() -> None:
    with patch("destination_dropbox.destination.DropboxClient"):
        with pytest.raises(RecordValidationError, match="Record 1 from stream 'other'"):
            list(
                DestinationDropbox().write(
                    CONFIG, _catalog("documents"), [_record("other", _data())]
                )
            )


def test_write_rejects_invalid_record_without_echoing_content() -> None:
    secret_content = "do-not-log-this-content"
    with patch("destination_dropbox.destination.DropboxClient"):
        with pytest.raises(RecordValidationError) as error:
            list(
                DestinationDropbox().write(
                    CONFIG,
                    _catalog("documents"),
                    [_record("documents", {"path": "../escape", "content_base64": secret_content})],
                )
            )

    assert "documents" in str(error.value)
    assert secret_content not in str(error.value)


def test_check_rejects_an_unknown_conflict_policy() -> None:
    config = {**CONFIG, "conflict_policy": "rename"}
    result = DestinationDropbox().check(Mock(), config)

    assert result.status == Status.FAILED
    assert "conflict_policy" in result.message
