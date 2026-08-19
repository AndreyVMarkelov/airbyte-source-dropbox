from __future__ import annotations

from collections.abc import Callable, Mapping
from time import sleep
from typing import Any, NoReturn, TypeVar

import dropbox
from dropbox.exceptions import (
    ApiError,
    AuthError,
    BadInputError,
    InternalServerError,
    RateLimitError,
)
from dropbox.files import (
    CommitInfo,
    CreateFolderError,
    FolderMetadata,
    UploadError,
    UploadSessionAppendError,
    UploadSessionCursor,
    UploadSessionFinishError,
    UploadSessionLookupError,
    WriteMode,
)

from destination_dropbox.validation import ValidatedFileRecord, normalize_upload_settings

T = TypeVar("T")
MAX_REQUEST_RETRIES = 3
MAX_OFFSET_RECOVERIES = 2


class DropboxAuthenticationError(RuntimeError):
    """Raised when Dropbox credentials cannot authenticate."""


class DropboxRateLimitError(RuntimeError):
    """Raised when Dropbox rate-limits connection validation."""


class DropboxWriteError(RuntimeError):
    """Raised when Dropbox cannot create folders or upload a destination record."""


class DropboxConflictError(DropboxWriteError):
    """Raised when the strict conflict policy finds an existing Dropbox item."""


class DropboxRootPathError(DropboxWriteError):
    """Raised when a configured destination root is missing or is not a folder."""


class DropboxUploadSessionError(DropboxWriteError):
    """Raised when a sequential Dropbox upload session cannot complete safely."""


class DropboxClient:
    """Dropbox authentication and sequential file-upload boundary."""

    def __init__(
        self, config: Mapping[str, Any], *, sleeper: Callable[[float], None] = sleep
    ) -> None:
        credentials = config["credentials"]
        auth_type = credentials["auth_type"]
        common_kwargs = {"max_retries_on_error": 0, "max_retries_on_rate_limit": 0}
        if auth_type == "oauth2_pkce":
            self._client = dropbox.Dropbox(
                oauth2_refresh_token=credentials["refresh_token"],
                app_key=credentials["app_key"],
                **common_kwargs,
            )
        elif auth_type == "access_token":
            self._client = dropbox.Dropbox(
                oauth2_access_token=credentials["access_token"], **common_kwargs
            )
        else:
            raise ValueError(f"Unsupported auth_type: {auth_type}")
        self._ensured_folders: set[str] = set()
        self._upload_settings = normalize_upload_settings(config)
        self._sleeper = sleeper

    def current_account(self) -> Any:
        try:
            return self._with_retry("connection check", self._client.users_get_current_account)
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc)
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited the connection check.") from exc
        except InternalServerError as exc:
            raise DropboxWriteError("Dropbox failed the connection check.") from exc

    def verify_root_path(self, root_path: str) -> None:
        """Require a non-empty configured root to already exist as a Dropbox folder."""
        if root_path == "":
            return
        try:
            metadata = self._with_retry(
                "destination-root validation", lambda: self._client.files_get_metadata(root_path)
            )
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc, required_scope="files.metadata.read")
        except RateLimitError as exc:
            raise DropboxRateLimitError(
                "Dropbox rate limited destination-root validation."
            ) from exc
        except ApiError as exc:
            raise DropboxRootPathError(
                "Configured root_path does not exist or is not accessible."
            ) from exc
        except InternalServerError as exc:
            raise DropboxRootPathError(
                "Dropbox could not validate the configured root_path."
            ) from exc
        if not isinstance(metadata, FolderMetadata):
            raise DropboxRootPathError("Configured root_path must refer to a Dropbox folder.")

    def upload_file(self, record: ValidatedFileRecord, conflict_policy: str, root_path: str) -> Any:
        """Create missing parent folders and upload a validated, bounded record."""
        self._ensure_parent_folders(record.destination_path, root_path)
        if len(record.content) > self._upload_settings.session_threshold_bytes:
            return self._upload_session(record, conflict_policy)
        return self._upload_small(record, conflict_policy)

    def _upload_small(self, record: ValidatedFileRecord, conflict_policy: str) -> Any:
        mode = WriteMode.overwrite if conflict_policy == "overwrite" else WriteMode.add
        try:
            return self._with_retry(
                "upload",
                lambda: self._client.files_upload(
                    record.content,
                    record.destination_path,
                    mode=mode,
                    autorename=False,
                    strict_conflict=conflict_policy == "fail",
                ),
            )
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc, required_scope="files.content.write")
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited file upload.") from exc
        except ApiError as exc:
            if self._is_upload_conflict(exc.error):
                raise DropboxConflictError(
                    "Dropbox found an existing item at the destination path."
                ) from exc
            raise DropboxWriteError("Dropbox failed to upload the file.") from exc
        except InternalServerError as exc:
            raise DropboxWriteError("Dropbox failed to upload the file.") from exc

    def _upload_session(self, record: ValidatedFileRecord, conflict_policy: str) -> Any:
        content = memoryview(record.content)
        first_end = min(len(content), self._upload_settings.chunk_size_bytes)
        try:
            start = self._with_retry(
                "start", lambda: self._client.files_upload_session_start(bytes(content[:first_end]))
            )
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc, required_scope="files.content.write")
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited upload-session start.") from exc
        except (ApiError, InternalServerError) as exc:
            raise DropboxUploadSessionError("Dropbox upload session start failed.") from exc

        session_id = getattr(start, "session_id", None)
        if not isinstance(session_id, str) or not session_id:
            raise DropboxUploadSessionError(
                "Dropbox upload-session start returned an invalid result."
            )

        commit = self._commit_info(record.destination_path, conflict_policy)
        offset = first_end
        recoveries = 0
        while True:
            operation = "finish"
            try:
                if offset == len(content):
                    return self._finish_session(content, session_id, offset, commit)
                end = min(offset + self._upload_settings.chunk_size_bytes, len(content))
                if end == len(content):
                    return self._finish_session(content, session_id, offset, commit)
                operation = "append"
                self._append_session(content, session_id, offset, end)
                offset = end
            except ApiError as exc:
                corrected_offset = self._correct_offset(exc.error)
                if corrected_offset is None:
                    self._raise_session_api_error(exc, operation)
                if (
                    recoveries >= MAX_OFFSET_RECOVERIES
                    or corrected_offset < 0
                    or corrected_offset > len(content)
                    or corrected_offset == offset
                ):
                    raise DropboxUploadSessionError(
                        f"Dropbox upload-session {operation} returned an unusable offset recovery."
                    ) from exc
                offset = corrected_offset
                recoveries += 1

    def _append_session(self, content: memoryview, session_id: str, offset: int, end: int) -> None:
        try:
            self._with_retry(
                "append",
                lambda: self._client.files_upload_session_append_v2(
                    bytes(content[offset:end]), UploadSessionCursor(session_id, offset)
                ),
            )
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc, required_scope="files.content.write")
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited upload-session append.") from exc
        except InternalServerError as exc:
            raise DropboxUploadSessionError("Dropbox upload session append failed.") from exc

    def _finish_session(
        self, content: memoryview, session_id: str, offset: int, commit: CommitInfo
    ) -> Any:
        try:
            return self._with_retry(
                "finish",
                lambda: self._client.files_upload_session_finish(
                    bytes(content[offset:]), UploadSessionCursor(session_id, offset), commit
                ),
            )
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc, required_scope="files.content.write")
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited upload-session finish.") from exc
        except InternalServerError as exc:
            raise DropboxUploadSessionError("Dropbox upload session finish failed.") from exc

    @staticmethod
    def _commit_info(destination_path: str, conflict_policy: str) -> CommitInfo:
        return CommitInfo(
            path=destination_path,
            mode=WriteMode.overwrite if conflict_policy == "overwrite" else WriteMode.add,
            autorename=False,
            strict_conflict=conflict_policy == "fail",
        )

    def _with_retry(self, operation: str, action: Callable[[], T]) -> T:
        for retry in range(MAX_REQUEST_RETRIES + 1):
            try:
                return action()
            except (RateLimitError, InternalServerError) as exc:
                if retry == MAX_REQUEST_RETRIES:
                    raise
                delay = 2**retry
                if isinstance(exc, RateLimitError) and isinstance(exc.backoff, (int, float)):
                    delay = max(delay, exc.backoff)
                self._sleeper(min(delay, 30))
        raise AssertionError(f"Unreachable retry state for {operation}.")

    @staticmethod
    def _correct_offset(error: Any) -> int | None:
        if isinstance(error, UploadSessionAppendError) and error.is_incorrect_offset():
            return error.get_incorrect_offset().correct_offset
        if isinstance(error, UploadSessionFinishError) and error.is_lookup_failed():
            lookup = error.get_lookup_failed()
            if isinstance(lookup, UploadSessionLookupError) and lookup.is_incorrect_offset():
                return lookup.get_incorrect_offset().correct_offset
        return None

    def _raise_session_api_error(self, exc: ApiError, operation: str) -> NoReturn:
        if isinstance(exc.error, UploadSessionFinishError) and exc.error.is_path():
            if exc.error.get_path().is_conflict():
                raise DropboxConflictError(
                    "Dropbox found an existing item at the destination path during "
                    "upload-session finish."
                ) from exc
        tag = self._session_lookup_tag(exc.error)
        if tag == "closed":
            message = f"Dropbox upload-session {operation} is closed."
        elif tag == "not_found":
            message = f"Dropbox upload-session {operation} was not found."
        elif tag == "too_large":
            message = f"Dropbox upload-session {operation} exceeds Dropbox limits."
        elif tag is not None:
            message = f"Dropbox upload-session {operation} returned an invalid session state."
        else:
            message = f"Dropbox upload-session {operation} failed."
        raise DropboxUploadSessionError(message) from exc

    @staticmethod
    def _session_lookup_tag(error: Any) -> str | None:
        if isinstance(error, UploadSessionAppendError):
            return getattr(error, "_tag", None)
        if isinstance(error, UploadSessionFinishError) and error.is_lookup_failed():
            return getattr(error.get_lookup_failed(), "_tag", None)
        return None

    def _ensure_parent_folders(self, destination_path: str, root_path: str) -> None:
        segments = destination_path.split("/")[1:-1]
        root_segment_count = len(root_path.split("/")) - 1 if root_path else 0
        for index in range(root_segment_count + 1, len(segments) + 1):
            folder_path = "/" + "/".join(segments[:index])
            if folder_path in self._ensured_folders:
                continue
            try:
                self._with_retry(
                    "parent-folder creation",
                    lambda folder_path=folder_path: self._client.files_create_folder_v2(
                        folder_path, autorename=False
                    ),
                )
            except (AuthError, BadInputError) as exc:
                self._raise_auth_or_refresh_error(exc, required_scope="files.content.write")
            except RateLimitError as exc:
                raise DropboxRateLimitError("Dropbox rate limited parent-folder creation.") from exc
            except ApiError as exc:
                if not self._is_existing_folder_conflict(exc.error):
                    raise DropboxWriteError("Dropbox failed to create a parent folder.") from exc
            except InternalServerError as exc:
                raise DropboxWriteError("Dropbox failed to create a parent folder.") from exc
            self._ensured_folders.add(folder_path)

    def _raise_auth_or_refresh_error(
        self, exc: AuthError | BadInputError, *, required_scope: str | None = None
    ) -> None:
        if isinstance(exc, AuthError):
            raise DropboxAuthenticationError(self._auth_message(exc, required_scope)) from exc
        raise DropboxAuthenticationError(self._bad_input_message(exc)) from exc

    @staticmethod
    def _auth_message(exc: AuthError, required_scope: str | None = None) -> str:
        tag = getattr(exc.error, "_tag", None)
        if tag == "missing_scope":
            scope = required_scope or "account_info.read"
            return f"Dropbox credentials are missing the required {scope} scope."
        if tag in {"invalid_access_token", "expired_access_token"}:
            return "Dropbox refresh token or access token is invalid, expired, or revoked."
        return "Dropbox rejected the supplied credentials."

    @staticmethod
    def _bad_input_message(exc: BadInputError) -> str:
        message = exc.message.lower()
        if "invalid_client" in message:
            return "Dropbox rejected the configured app key."
        if "invalid_grant" in message:
            return "Dropbox rejected the refresh token. It may be invalid or revoked."
        return "Dropbox could not refresh the access token. Check the app key and refresh token."

    @staticmethod
    def _is_existing_folder_conflict(error: Any) -> bool:
        if not isinstance(error, CreateFolderError) or not error.is_path():
            return False
        reason = error.get_path()
        return reason.is_conflict() and reason.get_conflict().is_folder()

    @staticmethod
    def _is_upload_conflict(error: Any) -> bool:
        if not isinstance(error, UploadError) or not error.is_path():
            return False
        return error.get_path().reason.is_conflict()
