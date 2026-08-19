from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dropbox.exceptions import ApiError, AuthError, BadInputError, RateLimitError
from dropbox.files import (
    CreateFolderError,
    FolderMetadata,
    UploadError,
    UploadWriteFailed,
    WriteConflictError,
    WriteError,
    WriteMode,
)

from destination_dropbox.client import (
    DropboxAuthenticationError,
    DropboxClient,
    DropboxConflictError,
    DropboxRateLimitError,
    DropboxRootPathError,
    DropboxWriteError,
)
from destination_dropbox.validation import ValidatedFileRecord


def test_refresh_token_authentication_uses_app_key() -> None:
    config = {
        "credentials": {"auth_type": "oauth2_pkce", "app_key": "key", "refresh_token": "token"}
    }
    client = DropboxClient(config)
    assert client._client is not None


def test_current_account_classifies_errors() -> None:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    client._client.users_get_current_account.side_effect = AuthError(
        "request-id", SimpleNamespace(_tag="invalid_access_token")
    )
    with pytest.raises(DropboxAuthenticationError, match="invalid, expired, or revoked"):
        client.current_account()

    client._client.users_get_current_account.side_effect = BadInputError(
        "request-id", '{"error":"invalid_client"}'
    )
    with pytest.raises(DropboxAuthenticationError, match="app key"):
        client.current_account()

    client._client.users_get_current_account.side_effect = RateLimitError("request-id")
    with pytest.raises(DropboxRateLimitError, match="rate limited"):
        client.current_account()


def _write_client() -> DropboxClient:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    client._ensured_folders = set()
    return client


def _record() -> ValidatedFileRecord:
    return ValidatedFileRecord("/Exports/nested/report.pdf", b"report", None, None)


def _folder(path: str) -> FolderMetadata:
    return FolderMetadata(
        name="Exports", id="id:exports", path_lower=path.lower(), path_display=path
    )


def test_verify_root_path_requires_an_existing_folder() -> None:
    client = _write_client()
    client._client.files_get_metadata.return_value = _folder("/Exports")

    client.verify_root_path("/Exports")
    client._client.files_get_metadata.assert_called_once_with("/Exports")

    client._client.files_get_metadata.return_value = Mock()
    with pytest.raises(DropboxRootPathError, match="must refer to a Dropbox folder"):
        client.verify_root_path("/Exports")


def test_verify_root_path_classifies_missing_metadata_scope() -> None:
    client = _write_client()
    client._client.files_get_metadata.side_effect = AuthError(
        "request-id", SimpleNamespace(_tag="missing_scope")
    )

    with pytest.raises(DropboxAuthenticationError, match="files.metadata.read"):
        client.verify_root_path("/Exports")


def test_upload_creates_parents_and_uses_overwrite_policy() -> None:
    client = _write_client()

    client.upload_file(_record(), "overwrite", "/Exports")

    assert client._client.files_create_folder_v2.call_args_list == [
        (("/Exports/nested",), {"autorename": False}),
    ]
    client._client.files_upload.assert_called_once_with(
        b"report",
        "/Exports/nested/report.pdf",
        mode=WriteMode.overwrite,
        autorename=False,
        strict_conflict=False,
    )


def test_upload_reuses_known_parent_folders_and_uses_strict_fail_policy() -> None:
    client = _write_client()

    client.upload_file(_record(), "fail", "/Exports")
    client.upload_file(_record(), "fail", "/Exports")

    assert client._client.files_create_folder_v2.call_count == 1
    assert client._client.files_upload.call_args.kwargs["mode"] == WriteMode.add
    assert client._client.files_upload.call_args.kwargs["strict_conflict"] is True


def test_existing_parent_folder_conflict_is_safe() -> None:
    client = _write_client()
    conflict = CreateFolderError.path(WriteError.conflict(WriteConflictError.folder))
    client._client.files_create_folder_v2.side_effect = ApiError(
        "request-id", conflict, None, None
    )

    client.upload_file(_record(), "overwrite", "/Exports")

    client._client.files_upload.assert_called_once()


def test_upload_classifies_missing_write_scope_rate_limit_conflict_and_api_errors() -> None:
    client = _write_client()
    client._ensured_folders = {"/Exports/nested"}
    client._client.files_upload.side_effect = AuthError(
        "request-id", SimpleNamespace(_tag="missing_scope")
    )
    with pytest.raises(DropboxAuthenticationError, match="files.content.write"):
        client.upload_file(_record(), "overwrite", "/Exports")

    client._client.files_upload.side_effect = RateLimitError("request-id")
    with pytest.raises(DropboxRateLimitError, match="rate limited"):
        client.upload_file(_record(), "overwrite", "/Exports")

    conflict = UploadError.path(
        UploadWriteFailed(WriteError.conflict(WriteConflictError.file), "session")
    )
    client._client.files_upload.side_effect = ApiError("request-id", conflict, None, None)
    with pytest.raises(DropboxConflictError, match="existing item"):
        client.upload_file(_record(), "fail", "/Exports")

    client._client.files_upload.side_effect = ApiError("request-id", Mock(), None, None)
    with pytest.raises(DropboxWriteError, match="failed to upload"):
        client.upload_file(_record(), "overwrite", "/Exports")
