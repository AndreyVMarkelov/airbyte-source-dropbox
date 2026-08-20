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
    GetMetadataError,
    UploadSessionAppendError,
    UploadSessionCursor,
    UploadSessionFinishError,
    UploadSessionLookupError,
    WriteMode,
)

from destination_dropbox_files.validation import PropagationOperation, StagedFile


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
            start = self._call(
                "upload-session start",
                lambda: self._client.files_upload_session_start(first),
            )
            session_id = getattr(start, "session_id", None)
            if not isinstance(session_id, str) or not session_id:
                raise DropboxFilesWriteError("Dropbox upload-session start returned an invalid result.")
            offset, recoveries = len(first), 0
            while True:
                stream.seek(offset)
                remaining = file.size - offset
                payload = stream.read(min(self._chunk_size, remaining))
                operation = "finish" if remaining <= self._chunk_size else "append"
                try:
                    if operation == "finish":
                        return self._finish(
                            session_id, offset, payload, file.destination_path, conflict_policy
                        )
                    self._call(
                        "upload-session append",
                        lambda payload=payload, offset=offset: self._client.files_upload_session_append_v2(
                            payload, UploadSessionCursor(session_id, offset)
                        ),
                        passthrough_api=True,
                    )
                    offset += len(payload)
                except ApiError as exc:
                    corrected = self._correct_offset(exc.error)
                    if (
                        corrected is None
                        or recoveries >= 2
                        or corrected < 0
                        or corrected > file.size
                        or corrected == offset
                    ):
                        raise DropboxFilesWriteError(
                            f"Dropbox upload-session {operation} returned an unusable offset recovery."
                        ) from exc
                    offset, recoveries = corrected, recoveries + 1

    def apply_propagation(self, operation: PropagationOperation, root_path: str) -> None:
        old_path = _join_root(root_path, operation.old_path)
        if operation.kind == "delete":
            self._delete_file(old_path, operation.old_content_hash)
            return
        assert operation.new_path is not None and operation.new_content_hash is not None
        new_path = _join_root(root_path, operation.new_path)
        self._move_file(old_path, new_path, operation.old_content_hash, operation.new_content_hash, root_path)

    def _move_file(
        self, old_path: str, new_path: str, old_hash: str, new_hash: str, root_path: str
    ) -> None:
        old_metadata = self._get_metadata_or_none(old_path)
        new_metadata = self._get_metadata_or_none(new_path)
        if old_metadata is None:
            if new_metadata is not None and new_metadata.content_hash in {old_hash, new_hash}:
                return
            raise DropboxFilesWriteError("Dropbox rename source is unavailable or destination conflicts.")
        if old_metadata.content_hash != old_hash:
            raise DropboxFilesWriteError("Dropbox rename source content no longer matches migration state.")
        if new_metadata is not None:
            raise DropboxFilesConflictError("Dropbox rename destination already exists.")
        self._ensure_parents(new_path, root_path)
        self._call(
            "rename propagation",
            lambda: self._client.files_move_v2(old_path, new_path, autorename=False),
        )

    def _delete_file(self, path: str, expected_hash: str) -> None:
        metadata = self._get_metadata_or_none(path)
        if metadata is None:
            return
        if metadata.content_hash != expected_hash:
            raise DropboxFilesWriteError("Dropbox delete target content no longer matches migration state.")
        self._call("delete propagation", lambda: self._client.files_delete_v2(path))

    def _get_metadata_or_none(self, path: str) -> Any | None:
        try:
            metadata = self._call(
                "propagation metadata lookup",
                lambda: self._client.files_get_metadata(path),
                passthrough_api=True,
            )
        except ApiError as exc:
            if self._is_not_found(exc.error):
                return None
            raise DropboxFilesWriteError("Dropbox propagation metadata lookup failed.") from exc
        if not hasattr(metadata, "content_hash"):
            raise DropboxFilesWriteError("Dropbox propagation target is not a file.")
        return metadata

    def _finish(self, session_id: str, offset: int, payload: bytes, path: str, policy: str) -> Any:
        commit = CommitInfo(path=path, mode=WriteMode.overwrite if policy == "overwrite" else WriteMode.add, autorename=False, strict_conflict=policy == "fail")
        try:
            return self._call(
                "upload-session finish",
                lambda: self._client.files_upload_session_finish(
                    payload, UploadSessionCursor(session_id, offset), commit
                ),
                passthrough_api=True,
            )
        except ApiError as exc:
            if self._is_finish_conflict(exc.error):
                raise DropboxFilesConflictError("Dropbox found an existing destination file.") from exc
            raise

    def _ensure_parents(self, path: str, root_path: str) -> None:
        parts = path.split("/")[1:-1]
        start = len(root_path.split("/")) if root_path else 1
        for index in range(start, len(parts) + 1):
            folder = "/" + "/".join(parts[:index])
            if folder in self._folders:
                continue
            try:
                self._call(
                    "parent-folder creation",
                    lambda folder=folder: self._client.files_create_folder_v2(folder, autorename=False),
                    passthrough_api=True,
                )
            except ApiError as exc:
                if not self._folder_conflict(exc):
                    raise DropboxFilesWriteError("Dropbox could not create a parent folder.") from exc
            self._folders.add(folder)

    def _call(self, operation: str, call: Callable[[], Any], *, passthrough_api: bool = False) -> Any:
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
                if passthrough_api:
                    raise
                raise DropboxFilesWriteError(f"Dropbox {operation} failed.") from exc
        raise AssertionError("unreachable")

    @staticmethod
    def _folder_conflict(exc: ApiError) -> bool:
        error = exc.error
        return isinstance(error, CreateFolderError) and error.is_path() and error.get_path().is_conflict() and error.get_path().get_conflict().is_folder()

    @staticmethod
    def _correct_offset(error: Any) -> int | None:
        if isinstance(error, UploadSessionAppendError) and error.is_incorrect_offset():
            return error.get_incorrect_offset().correct_offset
        if isinstance(error, UploadSessionFinishError) and error.is_lookup_failed():
            lookup = error.get_lookup_failed()
            if isinstance(lookup, UploadSessionLookupError) and lookup.is_incorrect_offset():
                return lookup.get_incorrect_offset().correct_offset
        return None

    @staticmethod
    def _is_finish_conflict(error: Any) -> bool:
        return isinstance(error, UploadSessionFinishError) and error.is_path() and error.get_path().is_conflict()

    @staticmethod
    def _is_not_found(error: Any) -> bool:
        return (
            isinstance(error, GetMetadataError)
            and error.is_path()
            and error.get_path().is_not_found()
        )


def _join_root(root_path: str, relative_path: str) -> str:
    return f"{root_path}/{relative_path}" if root_path else f"/{relative_path}"
