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


def test_write_validates_configured_records_without_uploading() -> None:
    messages = [
        AirbyteMessage(type=Type.STATE),
        _record("documents", _data()),
    ]

    assert list(DestinationDropbox().write(CONFIG, _catalog("documents"), messages)) == []


def test_write_rejects_unknown_stream_with_identity() -> None:
    with pytest.raises(RecordValidationError, match="Record 1 from stream 'other'"):
        list(DestinationDropbox().write(CONFIG, _catalog("documents"), [_record("other", _data())]))


def test_write_rejects_invalid_record_without_echoing_content() -> None:
    secret_content = "do-not-log-this-content"
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
