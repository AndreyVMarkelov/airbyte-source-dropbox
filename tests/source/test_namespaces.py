from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from airbyte_cdk.models import SyncMode
from dropbox import riviera
from dropbox.files import FileMetadata

from source_dropbox.client import (
    DropboxClient,
    DropboxNamespaceError,
    DropboxPage,
    MarkdownExtraction,
    NamespaceInfo,
)
from source_dropbox.streams.entries import Entries
from source_dropbox.streams.file_contents import FileContents

CONFIG = {
    "credentials": {"auth_type": "access_token", "access_token": "test-token"},
    "path": "/Reports",
    "recursive": True,
    "file_contents": {
        "allowed_extensions": [".pdf"],
        "max_file_size_mb": 1,
        "timeout_seconds": 10,
    },
}


def _file(namespace_path: str = "/reports/report.pdf") -> FileMetadata:
    return FileMetadata(
        name="report.pdf",
        id="id:file",
        client_modified=datetime(2026, 8, 1, tzinfo=UTC),
        server_modified=datetime(2026, 8, 1, 1, tzinfo=UTC),
        rev="012345678",
        size=10,
        path_lower=namespace_path,
        path_display=namespace_path,
        content_hash="a" * 64,
        is_downloadable=True,
    )


def _namespace(namespace_id: str, *, name: str | None = None) -> NamespaceInfo:
    return NamespaceInfo(
        namespace_id=namespace_id,
        name=name,
        namespace_type="team_folder",
    )


def test_selected_namespace_ids_are_validated_and_sorted() -> None:
    client = DropboxClient.__new__(DropboxClient)
    namespaces = client._resolve_namespaces(
        {
            **CONFIG,
            "namespace_selection": {
                "mode": "selected",
                "namespace_ids": ["456", "123"],
            },
        }
    )

    assert [namespace.namespace_id for namespace in namespaces] == ["123", "456"]

    with pytest.raises(DropboxNamespaceError, match="duplicates"):
        client._resolve_namespaces(
            {
                **CONFIG,
                "namespace_selection": {
                    "mode": "selected",
                    "namespace_ids": ["123", "123"],
                },
            }
        )


def test_all_accessible_discovers_and_normalizes_namespaces() -> None:
    sdk = Mock()
    namespace_type = SimpleNamespace(_tag="team_folder")
    sdk.team_namespaces_list.return_value = SimpleNamespace(
        namespaces=[
            SimpleNamespace(namespace_id="456", name="B", namespace_type=namespace_type)
        ],
        cursor="next",
        has_more=True,
    )
    sdk.team_namespaces_list_continue.return_value = SimpleNamespace(
        namespaces=[
            SimpleNamespace(namespace_id="123", name="A", namespace_type=namespace_type)
        ],
        cursor=None,
        has_more=False,
    )
    client = DropboxClient.__new__(DropboxClient)
    client._common_kwargs = {}

    with patch("source_dropbox.client.build_dropbox_team_client", return_value=sdk):
        namespaces = client._list_accessible_namespaces(CONFIG)

    assert [namespace.provenance() for namespace in namespaces] == [
        {"namespace_id": "123", "namespace_name": "A", "namespace_type": "team_folder"},
        {"namespace_id": "456", "namespace_name": "B", "namespace_type": "team_folder"},
    ]


def test_selected_namespaces_traverse_each_rooted_client_once() -> None:
    first = Mock()
    second = Mock()
    first.files_list_folder.return_value = SimpleNamespace(
        entries=["a"], cursor="cursor-a", has_more=False
    )
    second.files_list_folder.return_value = SimpleNamespace(
        entries=["b"], cursor="cursor-b", has_more=False
    )
    client = DropboxClient.__new__(DropboxClient)
    client._config = {
        **CONFIG,
        "namespace_selection": {"mode": "selected", "namespace_ids": ["456", "123"]},
    }
    client._namespaces = [_namespace("123"), _namespace("456")]
    client._namespace_clients = {"123": first, "456": second}

    pages = list(client.iter_entries(path="/Reports", recursive=True, include_deleted=False))

    assert [(page.namespace.namespace_id, page.entries) for page in pages if page.namespace] == [
        ("123", ["a"]),
        ("456", ["b"]),
    ]
    first.files_list_folder.assert_called_once_with(
        path="/Reports", recursive=True, include_deleted=False
    )
    second.files_list_folder.assert_called_once_with(
        path="/Reports", recursive=True, include_deleted=False
    )


def test_entries_checkpoint_cursors_are_isolated_by_namespace() -> None:
    client = Mock()
    client.is_multi_namespace = True
    client.context_scope.return_value = {
        "team_mode": "user",
        "selected_member_id": "dbmid:member",
        "path_root_mode": "default",
    }
    stream = Entries(client, CONFIG)
    stream._pages["123:cursor-a"] = (["a"], _namespace("123", name="A"), "cursor-a")
    stream._pages["456:cursor-b"] = (["b"], _namespace("456", name="B"), "cursor-b")

    with patch(
        "source_dropbox.streams.entries.normalize_entry",
        side_effect=[{"entry_key": "a"}, {"entry_key": "b"}],
    ):
        first_records = list(
            stream.read_records(
                SyncMode.incremental, stream_slice={"cursor": "123:cursor-a"}
            )
        )
        assert first_records == [
            {
                "entry_key": "123:a",
                "namespace_id": "123",
                "namespace_name": "A",
                "namespace_type": "team_folder",
            }
        ]
        second_records = list(
            stream.read_records(
                SyncMode.incremental, stream_slice={"cursor": "456:cursor-b"}
            )
        )
        assert second_records == [
            {
                "entry_key": "456:b",
                "namespace_id": "456",
                "namespace_name": "B",
                "namespace_type": "team_folder",
            }
        ]

    assert stream.state["namespaces"] == {
        "123": {"cursor": "cursor-a"},
        "456": {"cursor": "cursor-b"},
    }


def test_file_contents_state_is_isolated_by_namespace_for_same_file_id() -> None:
    client = Mock()
    client.is_multi_namespace = True
    client.context_scope.return_value = {
        "team_mode": "user",
        "selected_member_id": "dbmid:member",
        "path_root_mode": "default",
    }
    client.iter_entries.return_value = [
        DropboxPage(entries=[_file()], cursor="a", has_more=False, namespace=_namespace("123")),
        DropboxPage(entries=[_file()], cursor="b", has_more=False, namespace=_namespace("456")),
    ]
    client.extract_markdown.return_value = MarkdownExtraction("# Report", "succeeded")

    stream = FileContents(client, CONFIG)
    records = []
    for stream_slice in stream.stream_slices(sync_mode=SyncMode.incremental):
        records.extend(
            stream.read_records(SyncMode.incremental, stream_slice=stream_slice)
        )

    assert [record["namespace_id"] for record in records] == ["123", "456"]
    assert set(stream.state["namespaces"]) == {"123", "456"}
    assert client.extract_markdown.call_count == 2


def test_file_contents_extracts_markdown_through_namespace_rooted_clients() -> None:
    default = Mock()
    first = Mock()
    second = Mock()
    first.riviera_get_markdown_async.return_value = SimpleNamespace(
        is_async_job_id=lambda: True,
        get_async_job_id=lambda: "job-a",
    )
    second.riviera_get_markdown_async.return_value = SimpleNamespace(
        is_async_job_id=lambda: True,
        get_async_job_id=lambda: "job-b",
    )
    first.riviera_get_markdown_async_check.return_value = _complete_markdown("# A")
    second.riviera_get_markdown_async_check.return_value = _complete_markdown("# B")

    client = DropboxClient.__new__(DropboxClient)
    client._client = default
    client._namespace_clients = {"123": first, "456": second}
    client._monotonic_clock = Mock(return_value=0)
    client._sleeper = Mock()

    assert client.extract_markdown("id:a", 10, namespace_id="123").markdown == "# A"
    assert client.extract_markdown("id:b", 10, namespace_id="456").markdown == "# B"

    first.riviera_get_markdown_async.assert_called_once()
    first.riviera_get_markdown_async_check.assert_called_once_with("job-a")
    second.riviera_get_markdown_async.assert_called_once()
    second.riviera_get_markdown_async_check.assert_called_once_with("job-b")
    default.riviera_get_markdown_async.assert_not_called()
    default.riviera_get_markdown_async_check.assert_not_called()


def _complete_markdown(markdown: str) -> object:
    return riviera.GetMarkdownAsyncCheckResult.complete(
        riviera.GetMarkdownResult(markdown=markdown)
    )
