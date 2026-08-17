import json
from pathlib import Path
from unittest.mock import Mock, patch

from airbyte_cdk.models import SyncMode

from source_dropbox.client import DropboxPage
from source_dropbox.source import SourceDropbox

CONFIG = {
    "credentials": {
        "auth_type": "access_token",
        "access_token": "test-token",
    }
}


def test_check_connection_success() -> None:
    with patch("source_dropbox.source.DropboxClient") as client_cls:
        client_cls.return_value.current_account.return_value = Mock()
        ok, error = SourceDropbox().check_connection(Mock(), CONFIG)

    assert ok is True
    assert error is None


def test_check_connection_failure() -> None:
    with patch("source_dropbox.source.DropboxClient") as client_cls:
        client_cls.return_value.current_account.side_effect = RuntimeError("bad token")
        ok, error = SourceDropbox().check_connection(Mock(), CONFIG)

    assert ok is False
    assert "bad token" in error


def test_streams_exposes_entries() -> None:
    with patch("source_dropbox.source.DropboxClient"):
        streams = SourceDropbox().streams(CONFIG)

    assert [stream.name for stream in streams] == ["entries"]
    assert streams[0].supports_incremental is True


def test_entries_reads_from_saved_cursor() -> None:
    entry = Mock()
    page = DropboxPage(entries=[entry], cursor="next-cursor", has_more=False)
    client = Mock()
    client.iter_entries.return_value = [page]

    with (
        patch("source_dropbox.source.DropboxClient", return_value=client),
        patch(
            "source_dropbox.streams.entries.normalize_entry",
            return_value={"entry_key": "file:1"},
        ),
    ):
        stream = SourceDropbox().streams(CONFIG)[0]
        records = list(stream.read_records(SyncMode.incremental, stream_state={"cursor": "saved"}))

    assert records == [{"entry_key": "file:1", "cursor": "next-cursor"}]
    client.iter_entries.assert_called_once_with(
        path="", recursive=True, include_deleted=True, cursor="saved"
    )
    assert stream.get_updated_state({}, records[-1]) == {"cursor": "next-cursor"}


def test_spec_declares_supported_authentication_shapes() -> None:
    spec_path = Path(__file__).parents[1] / "source_dropbox" / "spec.json"
    spec = json.loads(spec_path.read_text())
    credentials = spec["connectionSpecification"]["properties"]["credentials"]

    assert spec["connectionSpecification"]["required"] == ["credentials"]
    assert {item["properties"]["auth_type"]["const"] for item in credentials["oneOf"]} == {
        "oauth2_pkce",
        "access_token",
    }
