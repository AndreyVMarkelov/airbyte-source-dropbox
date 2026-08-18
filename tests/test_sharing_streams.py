import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from airbyte_cdk.models import (
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)
from dropbox.exceptions import ApiError, AuthError, BadInputError, RateLimitError
from dropbox.sharing import ListFoldersContinueError, ListSharedLinksError
from jsonschema import Draft7Validator

from source_dropbox.client import (
    DropboxAuthenticationError,
    DropboxClient,
    DropboxRateLimitError,
    DropboxSharedFoldersCursorResetError,
    DropboxSharedLinksCursorResetError,
    DropboxSharingPermissionError,
    SharedFoldersPage,
    SharedLinksPage,
)
from source_dropbox.source import SourceDropbox
from source_dropbox.streams.shared_folders import SharedFolders
from source_dropbox.streams.shared_links import SharedLinks

CONFIG = {"credentials": {"auth_type": "access_token", "access_token": "test-token"}}


def _tag(value: str) -> SimpleNamespace:
    return SimpleNamespace(_tag=value)


def _link() -> SimpleNamespace:
    return SimpleNamespace(
        url="https://www.dropbox.com/scl/fi/example/document.txt?rlkey=key",
        id="id:document",
        name="document.txt",
        path_lower="/document.txt",
        expires=datetime(2026, 9, 1, tzinfo=UTC),
        link_permissions=SimpleNamespace(
            resolved_visibility=_tag("public"),
            allow_download=True,
            effective_audience=_tag("public"),
            requested_visibility=_tag("public"),
            link_access_level=_tag("viewer"),
        ),
        team_member_info=SimpleNamespace(
            display_name="Dropbox User",
            member_id="dbmid:member",
            team_info=SimpleNamespace(id="dbtid:team", name="Example Team"),
        ),
        content_owner_team_info=SimpleNamespace(id="dbtid:owner", name="Owner Team"),
    )


def _folder() -> SimpleNamespace:
    return SimpleNamespace(
        shared_folder_id="shared:folder",
        folder_id="id:folder",
        name="Shared folder",
        path_lower=None,
        path_display=None,
        access_type=_tag("editor"),
        is_inside_team_folder=True,
        is_team_folder=False,
        preview_url="https://www.dropbox.com/home/Shared%20folder",
        time_invited=datetime(2026, 9, 2, tzinfo=UTC),
        parent_shared_folder_id=None,
        owner_team=SimpleNamespace(id="dbtid:team", name="Example Team"),
        policy=SimpleNamespace(
            acl_update_policy=_tag("editors"),
            shared_link_policy=_tag("anyone"),
            member_policy=_tag("anyone"),
            resolved_member_policy=_tag("team"),
            viewer_info_policy=None,
        ),
    )


def _schema(name: str) -> dict[str, object]:
    path = Path(__file__).parents[1] / "source_dropbox" / "schemas" / f"{name}.json"
    return json.loads(path.read_text())


def test_sharing_streams_normalize_paginated_structured_metadata() -> None:
    client = Mock()
    client.iter_shared_links.return_value = [
        SharedLinksPage(links=[_link()], cursor="next", has_more=True),
        SharedLinksPage(links=[_link()], cursor=None, has_more=False),
    ]
    client.iter_shared_folders.return_value = [
        SharedFoldersPage(entries=[_folder()], cursor="next"),
        SharedFoldersPage(entries=[_folder()], cursor=None),
    ]

    links = list(SharedLinks(client, CONFIG).read_records(SyncMode.full_refresh))
    folders = list(SharedFolders(client, CONFIG).read_records(SyncMode.full_refresh))

    assert len(links) == len(folders) == 2
    assert links[0]["link_key"] == links[0]["url"]
    assert links[0]["team_member_info"]["team_info"]["id"] == "dbtid:team"
    assert folders[0]["path_lower"] is None
    assert folders[0]["policy"] == {
        "acl_update_policy": "editors",
        "shared_link_policy": "anyone",
        "member_policy": "anyone",
        "resolved_member_policy": "team",
        "viewer_info_policy": None,
    }
    assert list(Draft7Validator(_schema("shared_links")).iter_errors(links[0])) == []
    assert list(Draft7Validator(_schema("shared_folders")).iter_errors(folders[0])) == []


def test_sharing_streams_allow_absent_optional_metadata() -> None:
    link = _link()
    link.expires = link.team_member_info = link.content_owner_team_info = None
    link.link_permissions = None
    folder = _folder()
    folder.owner_team = folder.policy = None
    client = Mock()
    client.iter_shared_links.return_value = [
        SharedLinksPage(links=[link], cursor=None, has_more=False)
    ]
    client.iter_shared_folders.return_value = [SharedFoldersPage(entries=[folder], cursor=None)]

    link_record = list(SharedLinks(client, CONFIG).read_records(SyncMode.full_refresh))[0]
    folder_record = list(SharedFolders(client, CONFIG).read_records(SyncMode.full_refresh))[0]
    assert link_record["expires"] is None
    assert link_record["visibility"] is None
    assert link_record["allow_download"] is None
    assert folder_record["policy"] is None


def test_sharing_streams_read_through_source_protocol() -> None:
    client = Mock()
    client.iter_shared_links.return_value = [
        SharedLinksPage(links=[_link()], cursor=None, has_more=False)
    ]
    client.iter_shared_folders.return_value = [SharedFoldersPage(entries=[_folder()], cursor=None)]
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=name,
                    json_schema=_schema(name),
                    supported_sync_modes=[SyncMode.full_refresh],
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
                primary_key=[[key]],
            )
            for name, key in (("shared_links", "link_key"), ("shared_folders", "shared_folder_id"))
        ]
    )
    with patch("source_dropbox.source.DropboxClient", return_value=client):
        messages = list(SourceDropbox().read(Mock(), CONFIG, catalog))
    records = [message.record for message in messages if message.type == Type.RECORD]
    assert [(record.stream, record.data["name"]) for record in records] == [
        ("shared_links", "document.txt"),
        ("shared_folders", "Shared folder"),
    ]


def test_shared_link_client_paginates_and_classifies_errors() -> None:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    client._client.sharing_list_shared_links.side_effect = [
        SimpleNamespace(links=[_link()], cursor="next", has_more=True),
        SimpleNamespace(links=[_link()], cursor=None, has_more=False),
    ]
    assert len(list(client.iter_shared_links())) == 2
    assert client._client.sharing_list_shared_links.call_args_list[1].kwargs == {"cursor": "next"}

    client._client.sharing_list_shared_links.side_effect = AuthError(
        "request-id", SimpleNamespace(_tag="missing_scope")
    )
    with pytest.raises(DropboxSharingPermissionError, match="sharing.read"):
        list(client.iter_shared_links())

    client._client.sharing_list_shared_links.side_effect = BadInputError(
        "request-id",
        "Your app is not permitted to access this endpoint because it does not have "
        "the required scope 'sharing.read'.",
    )
    with pytest.raises(DropboxSharingPermissionError, match="sharing.read"):
        list(client.iter_shared_links())

    client._client.sharing_list_shared_links.side_effect = BadInputError(
        "request-id", '{"error":"invalid_grant"}'
    )
    with pytest.raises(DropboxAuthenticationError, match="invalid or revoked"):
        list(client.iter_shared_links())
    client._client.sharing_list_shared_links.side_effect = RateLimitError("request-id")
    with pytest.raises(DropboxRateLimitError, match="sharing"):
        list(client.iter_shared_links())


def test_shared_link_client_restarts_once_after_cursor_reset() -> None:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    reset = ApiError("request-id", ListSharedLinksError.reset, None, None)
    client._client.sharing_list_shared_links.side_effect = [
        SimpleNamespace(links=[_link()], cursor="next", has_more=True),
        reset,
        SimpleNamespace(links=[_link()], cursor=None, has_more=False),
    ]

    assert len(list(client.iter_shared_links())) == 2
    assert client._client.sharing_list_shared_links.call_args_list[2].kwargs == {"cursor": None}


def test_shared_link_client_stops_after_a_second_cursor_reset() -> None:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    reset = ApiError("request-id", ListSharedLinksError.reset, None, None)
    client._client.sharing_list_shared_links.side_effect = [
        SimpleNamespace(links=[], cursor="next", has_more=True),
        reset,
        reset,
    ]

    with pytest.raises(DropboxSharedLinksCursorResetError, match="repeatedly"):
        list(client.iter_shared_links())


def test_shared_folder_client_paginates() -> None:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    client._client.sharing_list_folders.return_value = SimpleNamespace(
        entries=[_folder()], cursor="next"
    )
    client._client.sharing_list_folders_continue.return_value = SimpleNamespace(
        entries=[_folder()], cursor=None
    )
    assert len(list(client.iter_shared_folders())) == 2
    client._client.sharing_list_folders_continue.assert_called_once_with("next")


def test_shared_folder_client_classifies_refresh_token_failure_during_sync() -> None:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    client._client.sharing_list_folders.side_effect = BadInputError(
        "request-id", '{"error":"invalid_grant"}'
    )

    with pytest.raises(DropboxAuthenticationError, match="invalid or revoked"):
        list(client.iter_shared_folders())


def test_shared_folder_client_restarts_once_after_invalid_cursor() -> None:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    invalid_cursor = ApiError("request-id", ListFoldersContinueError.invalid_cursor, None, None)
    client._client.sharing_list_folders.side_effect = [
        SimpleNamespace(entries=[_folder()], cursor="next"),
        SimpleNamespace(entries=[_folder()], cursor=None),
    ]
    client._client.sharing_list_folders_continue.side_effect = invalid_cursor

    assert len(list(client.iter_shared_folders())) == 2
    assert client._client.sharing_list_folders.call_count == 2


def test_shared_folder_client_stops_after_a_second_invalid_cursor() -> None:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    invalid_cursor = ApiError("request-id", ListFoldersContinueError.invalid_cursor, None, None)
    client._client.sharing_list_folders.side_effect = [
        SimpleNamespace(entries=[], cursor="first"),
        SimpleNamespace(entries=[], cursor="second"),
    ]
    client._client.sharing_list_folders_continue.side_effect = invalid_cursor

    with pytest.raises(DropboxSharedFoldersCursorResetError, match="repeatedly"):
        list(client.iter_shared_folders())
