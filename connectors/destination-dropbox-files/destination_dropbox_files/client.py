from __future__ import annotations

from collections.abc import Callable, Mapping
from time import sleep
from typing import Any

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
    UploadSessionCursor,
    WriteMode,
)

from destination_dropbox_files.validation import StagedFile


class DropboxFilesWriteError(RuntimeError):
    pass


class DropboxFilesAuthenticationError(DropboxFilesWriteError):
    pass


class DropboxFilesConflictError(DropboxFilesWriteError):
    pass


class DropboxFilesClient:
    def __init__(self, config: Mapping[str, Any], *, sleeper: Callable[[float], None] = sleep) -> None:
        credentials = config["credentials"]
        kwargs = {"max_retries_on_error": 0, "max_retries_on_rate_limit": 0}
        if credentials["auth_type"] == "oauth2_pkce":
            self._client = dropbox.Dropbox(oauth2_refresh_token=credentials["refresh_token"], app_key=credentials["app_key"], **kwargs)
        else:
            self._client = dropbox.Dropbox(oauth2_access_token=credentials["access_token"], **kwargs)
        self._chunk_size = int(config.get("upload_chunk_size_mb", 8)) * 1024 * 1024
        if not 1024 * 1024 <= self._chunk_size <= 16 * 1024 * 1024:
            raise DropboxFilesWriteError("upload_chunk_size_mb must be between 1 and 16.")
        self._folders: set[str] = set()
        self._sleeper = sleeper

    def current_account(self) -> Any:
        return self._call("connection check", self._client.users_get_current_account)

    def verify_root_path(self, root_path: str) -> None:
        if not root_path:
            return
        metadata = self._call("root validation", lambda: self._client.files_get_metadata(root_path))
        if not isinstance(metadata, FolderMetadata):
            raise DropboxFilesWriteError("Configured root_path must refer to an existing Dropbox folder.")

    def upload_staged_file(self, file: StagedFile, root_path: str, conflict_policy: str) -> Any:
        self._ensure_parents(file.destination_path, root_path)
        with file.path.open("rb") as stream:
            first = stream.read(self._chunk_size)
            start = self._call("upload-session start", lambda: self._client.files_upload_session_start(first))
            session_id = getattr(start, "session_id", None)
            if not isinstance(session_id, str) or not session_id:
                raise DropboxFilesWriteError("Dropbox upload-session start returned an invalid result.")
            offset = len(first)
            while True:
                chunk = stream.read(self._chunk_size)
                if not chunk:
                    return self._finish(session_id, offset, b"", file.destination_path, conflict_policy)
                next_chunk = stream.read(self._chunk_size)
                if not next_chunk:
                    return self._finish(session_id, offset, chunk, file.destination_path, conflict_policy)
                self._call("upload-session append", lambda: self._client.files_upload_session_append_v2(chunk, UploadSessionCursor(session_id, offset)))
                offset += len(chunk)
                stream.seek(-len(next_chunk), 1)

    def _finish(self, session_id: str, offset: int, payload: bytes, path: str, policy: str) -> Any:
        commit = CommitInfo(path=path, mode=WriteMode.overwrite if policy == "overwrite" else WriteMode.add, autorename=False, strict_conflict=policy == "fail")
        return self._call("upload-session finish", lambda: self._client.files_upload_session_finish(payload, UploadSessionCursor(session_id, offset), commit))

    def _ensure_parents(self, path: str, root_path: str) -> None:
        parts = path.split("/")[1:-1]
        start = len(root_path.split("/")) if root_path else 1
        for index in range(start, len(parts) + 1):
            folder = "/" + "/".join(parts[:index])
            if folder in self._folders:
                continue
            try:
                self._call("parent-folder creation", lambda folder=folder: self._client.files_create_folder_v2(folder, autorename=False))
            except ApiError as exc:
                if not self._folder_conflict(exc):
                    raise DropboxFilesWriteError("Dropbox could not create a parent folder.") from exc
            self._folders.add(folder)

    def _call(self, operation: str, call: Callable[[], Any]) -> Any:
        for attempt in range(4):
            try:
                return call()
            except (AuthError, BadInputError) as exc:
                raise DropboxFilesAuthenticationError("Dropbox credentials are invalid or missing required scope.") from exc
            except (RateLimitError, InternalServerError) as exc:
                if attempt == 3:
                    raise DropboxFilesWriteError(f"Dropbox {operation} retry budget was exhausted.") from exc
                self._sleeper(2**attempt)
            except ApiError as exc:
                raise DropboxFilesWriteError(f"Dropbox {operation} failed.") from exc
        raise AssertionError("unreachable")

    @staticmethod
    def _folder_conflict(exc: ApiError) -> bool:
        error = exc.error
        return isinstance(error, CreateFolderError) and error.is_path() and error.get_path().is_conflict() and error.get_path().get_conflict().is_folder()
