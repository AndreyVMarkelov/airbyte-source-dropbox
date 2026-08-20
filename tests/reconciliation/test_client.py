from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import dropbox_reconciliation.client as reconciliation_client
from dropbox_reconciliation.client import (
    DropboxReconciliationClient,
    DuplicateNormalizedPathError,
)


class FakeFile:
    def __init__(self, path_lower: str, path_display: str, content_hash: str = "hash") -> None:
        self.path_lower = path_lower
        self.path_display = path_display
        self.id = "id:file"
        self.rev = "rev"
        self.size = 4
        self.content_hash = content_hash
        self.client_modified = None
        self.server_modified = None


def _client() -> DropboxReconciliationClient:
    client = DropboxReconciliationClient.__new__(DropboxReconciliationClient)
    client.side = "source"
    client._client = Mock()
    return client


def test_clients_are_built_from_independent_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = Mock()
    monkeypatch.setattr(reconciliation_client.dropbox, "Dropbox", factory)

    DropboxReconciliationClient(
        {"credentials": {"auth_type": "access_token", "access_token": "source-token"}}, "source"
    )
    DropboxReconciliationClient(
        {"credentials": {"auth_type": "access_token", "access_token": "destination-token"}},
        "destination",
    )

    assert factory.call_args_list[0].kwargs["oauth2_access_token"] == "source-token"
    assert factory.call_args_list[1].kwargs["oauth2_access_token"] == "destination-token"


def test_inventory_paginates_and_excludes_non_files(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reconciliation_client, "FileMetadata", FakeFile)
    client = _client()
    client._client.files_list_folder.return_value = SimpleNamespace(
        entries=[FakeFile("/root/first.txt", "/root/first.txt"), object()],
        cursor="next",
        has_more=True,
    )
    client._client.files_list_folder_continue.return_value = SimpleNamespace(
        entries=[FakeFile("/root/nested/second.txt", "/root/nested/second.txt")],
        cursor="done",
        has_more=False,
    )

    inventory = client.inventory("/root")

    assert list(inventory.files) == ["first.txt", "nested/second.txt"]
    client._client.files_list_folder.assert_called_once_with(
        "/root", recursive=True, include_deleted=False
    )
    client._client.files_list_folder_continue.assert_called_once_with("next")
    client._client.files_download.assert_not_called()


def test_inventory_records_invalid_relative_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reconciliation_client, "FileMetadata", FakeFile)
    client = _client()
    client._client.files_list_folder.return_value = SimpleNamespace(
        entries=[FakeFile("/other/file.txt", "/other/file.txt")], cursor="done", has_more=False
    )

    inventory = client.inventory("/root")

    assert not inventory.files
    assert inventory.issues[0].path == "/other/file.txt"


def test_inventory_rejects_duplicate_normalized_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(reconciliation_client, "FileMetadata", FakeFile)
    client = _client()
    client._client.files_list_folder.return_value = SimpleNamespace(
        entries=[
            FakeFile("/root/File.txt", "/root/File.txt"),
            FakeFile("/root/file.txt", "/root/file.txt"),
        ],
        cursor="done",
        has_more=False,
    )

    with pytest.raises(DuplicateNormalizedPathError, match="duplicate_normalized_path"):
        client.inventory("/root")
