from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dropbox.exceptions import (
    ApiError,
    AuthError,
    BadInputError,
    InternalServerError,
    RateLimitError,
)
from dropbox.files import (
    CreateFolderError,
    FolderMetadata,
    UploadError,
    UploadSessionAppendError,
    UploadSessionFinishError,
    UploadSessionLookupError,
    UploadSessionOffsetError,
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
    DropboxUploadSessionError,
    DropboxWriteError,
)
from destination_dropbox.validation import UploadSettings, ValidatedFileRecord


def test_refresh_token_authentication_uses_app_key() -> None:
    config = {
        "credentials": {"auth_type": "oauth2_pkce", "app_key": "key", "refresh_token": "token"}
    }
    client = DropboxClient(config)
    assert client._client is not None


def test_current_account_classifies_errors() -> None:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    client._sleeper = Mock()
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


def _write_client(*, threshold: int = 8, chunk_size: int = 4) -> DropboxClient:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    client._ensured_folders = set()
    client._upload_settings = UploadSettings(64, threshold, chunk_size)
    client._sleeper = Mock()
    return client


def _record() -> ValidatedFileRecord:
    return ValidatedFileRecord("/Exports/nested/report.pdf", b"report", None, None)


def _sized_record(content: bytes) -> ValidatedFileRecord:
    return ValidatedFileRecord("/Exports/nested/report.pdf", content, None, None)


def _started_session_client(*, threshold: int = 4, chunk_size: int = 4) -> DropboxClient:
    client = _write_client(threshold=threshold, chunk_size=chunk_size)
    client._ensured_folders = {"/Exports/nested"}
    client._client.files_upload_session_start.return_value = SimpleNamespace(
        session_id="session-secret"
    )
    return client


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
    client._client.files_create_folder_v2.side_effect = ApiError("request-id", conflict, None, None)

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


def test_upload_uses_direct_transport_at_session_threshold() -> None:
    client = _write_client(threshold=4, chunk_size=4)
    client._ensured_folders = {"/Exports/nested"}

    client.upload_file(_sized_record(b"1234"), "overwrite", "/Exports")

    client._client.files_upload.assert_called_once()
    client._client.files_upload_session_start.assert_not_called()


def test_upload_session_orders_start_append_and_finish_with_offsets() -> None:
    client = _started_session_client()
    client._ensured_folders.clear()

    client.upload_file(_sized_record(b"0123456789"), "overwrite", "/Exports")

    client._client.files_create_folder_v2.assert_called_once()
    client._client.files_upload.assert_not_called()
    client._client.files_upload_session_start.assert_called_once_with(b"0123")
    append_payload, append_cursor = client._client.files_upload_session_append_v2.call_args.args
    assert append_payload == b"4567"
    assert append_cursor.offset == 4
    finish_call = client._client.files_upload_session_finish.call_args
    finish_payload, finish_cursor, commit = finish_call.args
    assert finish_payload == b"89"
    assert finish_cursor.offset == 8
    assert commit.path == "/Exports/nested/report.pdf"
    assert commit.mode == WriteMode.overwrite
    assert commit.autorename is False
    assert commit.strict_conflict is False


@pytest.mark.parametrize(
    ("content", "start", "finish"),
    [(b"12345", b"12345", b""), (b"12345678", b"1234", b"5678")],
)
def test_upload_session_supports_empty_finish_and_exact_chunk_boundary(
    content: bytes, start: bytes, finish: bytes
) -> None:
    client = _started_session_client(chunk_size=8 if len(content) == 5 else 4)

    client.upload_file(_sized_record(content), "fail", "/Exports")

    assert client._client.files_upload_session_start.call_args.args == (start,)
    assert client._client.files_upload_session_append_v2.call_count == 0
    finish_call = client._client.files_upload_session_finish.call_args
    finish_payload, finish_cursor, commit = finish_call.args
    assert finish_payload == finish
    assert finish_cursor.offset == len(start)
    assert commit.mode == WriteMode.add
    assert commit.strict_conflict is True


def test_upload_session_rejects_malformed_start_result_without_leaking_content() -> None:
    client = _write_client(threshold=4, chunk_size=4)
    client._ensured_folders = {"/Exports/nested"}
    client._client.files_upload_session_start.return_value = SimpleNamespace(session_id=None)

    with pytest.raises(DropboxUploadSessionError, match="invalid upload-session start") as raised:
        client.upload_file(_sized_record(b"private-content"), "overwrite", "/Exports")

    assert "private-content" not in str(raised.value)


def test_upload_session_recovers_from_forward_and_backward_offsets() -> None:
    client = _started_session_client()
    forward = UploadSessionAppendError.incorrect_offset(UploadSessionOffsetError(8))
    client._client.files_upload_session_append_v2.side_effect = ApiError(
        "request-id", forward, None, None
    )

    client.upload_file(_sized_record(b"0123456789"), "overwrite", "/Exports")

    finish_payload, finish_cursor, _ = client._client.files_upload_session_finish.call_args.args
    assert finish_payload == b"89"
    assert finish_cursor.offset == 8

    client = _started_session_client()
    client._client.files_upload_session_append_v2.side_effect = [
        None,
        ApiError(
            "request-id",
            UploadSessionAppendError.incorrect_offset(UploadSessionOffsetError(4)),
            None,
            None,
        ),
        None,
        None,
    ]

    client.upload_file(_sized_record(b"0123456789abcd"), "overwrite", "/Exports")

    append_calls = client._client.files_upload_session_append_v2.call_args_list
    assert [call.args[1].offset for call in append_calls] == [
        4,
        8,
        4,
        8,
    ]


def test_upload_session_recovery_to_complete_finishes_empty_and_is_bounded() -> None:
    client = _started_session_client()
    client._client.files_upload_session_append_v2.side_effect = ApiError(
        "request-id",
        UploadSessionAppendError.incorrect_offset(UploadSessionOffsetError(10)),
        None,
        None,
    )

    client.upload_file(_sized_record(b"0123456789"), "overwrite", "/Exports")

    assert client._client.files_upload_session_finish.call_args.args[0] == b""
    assert client._client.files_upload_session_finish.call_args.args[1].offset == 10

    client._client.files_upload_session_append_v2.side_effect = ApiError(
        "request-id",
        UploadSessionAppendError.incorrect_offset(UploadSessionOffsetError(4)),
        None,
        None,
    )
    with pytest.raises(DropboxUploadSessionError, match="unusable offset"):
        client.upload_file(_sized_record(b"0123456789"), "overwrite", "/Exports")


def test_upload_session_recovers_from_finish_incorrect_offset() -> None:
    client = _started_session_client()
    client._client.files_upload_session_finish.side_effect = [
        ApiError(
            "request-id",
            UploadSessionFinishError.lookup_failed(
                UploadSessionLookupError.incorrect_offset(UploadSessionOffsetError(10))
            ),
            None,
            None,
        ),
        None,
    ]

    client.upload_file(_sized_record(b"0123456789"), "overwrite", "/Exports")

    finish_calls = client._client.files_upload_session_finish.call_args_list
    assert [call.args[1].offset for call in finish_calls] == [
        8,
        10,
    ]
    assert client._client.files_upload_session_finish.call_args.args[0] == b""


@pytest.mark.parametrize(
    "error",
    [
        UploadSessionAppendError.closed,
        UploadSessionAppendError.not_found,
        UploadSessionAppendError.too_large,
        UploadSessionAppendError.other,
    ],
)
def test_upload_session_classifies_lookup_errors_without_session_leak(error: object) -> None:
    client = _started_session_client()
    client._client.files_upload_session_append_v2.side_effect = ApiError(
        "request-id", error, None, None
    )

    with pytest.raises(DropboxUploadSessionError) as raised:
        client.upload_file(_sized_record(b"0123456789"), "overwrite", "/Exports")

    assert "session-secret" not in str(raised.value)


def test_upload_session_classifies_finish_conflict_and_lookup_error() -> None:
    client = _started_session_client()
    conflict = UploadSessionFinishError.path(WriteError.conflict(WriteConflictError.file))
    client._client.files_upload_session_finish.side_effect = ApiError(
        "request-id", conflict, None, None
    )
    with pytest.raises(DropboxConflictError, match="existing item"):
        client.upload_file(_sized_record(b"0123456789"), "fail", "/Exports")

    client._client.files_upload_session_finish.side_effect = ApiError(
        "request-id",
        UploadSessionFinishError.lookup_failed(UploadSessionLookupError.closed),
        None,
        None,
    )
    with pytest.raises(DropboxUploadSessionError, match="closed"):
        client.upload_file(_sized_record(b"0123456789"), "overwrite", "/Exports")


@pytest.mark.parametrize(
    "exception",
    [RateLimitError("request-id"), InternalServerError("request-id", 500, "server error")],
)
def test_upload_session_retries_transient_start_errors(exception: Exception) -> None:
    client = _write_client(threshold=4, chunk_size=4)
    client._ensured_folders = {"/Exports/nested"}
    client._client.files_upload_session_start.side_effect = [
        exception,
        SimpleNamespace(session_id="session-secret"),
    ]

    client.upload_file(_sized_record(b"0123456789"), "overwrite", "/Exports")

    assert client._sleeper.call_args.args == (1,)
    assert client._client.files_upload_session_start.call_count == 2


def test_upload_session_transient_retry_exhaustion_and_auth_are_explicit() -> None:
    client = _write_client(threshold=4, chunk_size=4)
    client._ensured_folders = {"/Exports/nested"}
    client._client.files_upload_session_start.side_effect = [RateLimitError("request-id")] * 4
    with pytest.raises(DropboxRateLimitError, match="start"):
        client.upload_file(_sized_record(b"0123456789"), "overwrite", "/Exports")
    assert [call.args[0] for call in client._sleeper.call_args_list] == [1, 2, 4]

    client._client.files_upload_session_start.side_effect = AuthError(
        "request-id", SimpleNamespace(_tag="invalid_access_token")
    )
    with pytest.raises(DropboxAuthenticationError, match="invalid, expired, or revoked"):
        client.upload_file(_sized_record(b"0123456789"), "overwrite", "/Exports")
