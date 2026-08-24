from __future__ import annotations

import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from time import sleep
from typing import Any

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
    FileMetadata,
    FolderMetadata,
    UploadSessionAppendError,
    UploadSessionCursor,
    UploadSessionFinishError,
    UploadSessionLookupError,
    WriteMode,
)

from dropbox_repair.dropbox_context import DropboxContextError, build_dropbox_client


class RepairError(RuntimeError):
    pass


class SourceDriftError(RepairError):
    pass


class DropboxRepairClient:
    def __init__(self, config: Mapping[str, Any], side: str, *, sleeper=sleep) -> None:
        kwargs = {"max_retries_on_error": 0, "max_retries_on_rate_limit": 0}
        try:
            self._client = build_dropbox_client(config, **kwargs)
        except DropboxContextError as exc:
            raise RepairError(
                f"{side} config must contain a non-empty access_token, app_key, or refresh_token."
            ) from exc
        self.side = side
        self._folders: set[str] = set()
        self._sleeper = sleeper

    def validate_root(self, value: object) -> str:
        root = normalize_root(value)
        try:
            self._client.users_get_current_account()
            if root:
                metadata = self._client.files_get_metadata(root)
        except (AuthError, BadInputError) as exc:
            raise RepairError(f"{self.side} credentials are invalid or revoked.") from exc
        except (ApiError, RateLimitError, InternalServerError) as exc:
            raise RepairError(f"{self.side} root is unavailable.") from exc
        if root and not isinstance(metadata, FolderMetadata):
            raise RepairError(f"{self.side} root_path must be a Dropbox folder.")
        return root

    def stage_source_file(
        self, source: Mapping[str, Any], source_root: str, relative_path: str, chunk_size: int
    ) -> Path:
        try:
            metadata, response = self._call(
                lambda: self._client.files_download(source["file_id"]),
                "source file download",
                passthrough_api=True,
            )
        except ApiError as exc:
            raise SourceDriftError("source file is unavailable for repair.") from exc
        if (
            not isinstance(metadata, FileMetadata)
            or not _metadata_matches(metadata, source)
            or not _is_under_root(metadata.path_lower, source_root, relative_path)
        ):
            raise SourceDriftError("source file changed since reconciliation.")
        descriptor, temporary_name = tempfile.mkstemp(prefix="dropbox-repair-")
        staged = Path(temporary_name)
        bytes_written = 0
        try:
            with os.fdopen(descriptor, "wb") as output:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        output.write(chunk)
                        bytes_written += len(chunk)
            if bytes_written != source["size"]:
                raise SourceDriftError("source file size changed during download.")
            return staged
        except Exception:
            staged.unlink(missing_ok=True)
            raise

    def upload_staged_file(
        self, staged: Path, destination_path: str, root: str, chunk_size: int
    ) -> None:
        self._ensure_parents(destination_path, root)
        size = staged.stat().st_size
        with staged.open("rb") as stream:
            first = stream.read(chunk_size)
            start = self._call(
                lambda: self._client.files_upload_session_start(first), "upload session start"
            )
            session_id = getattr(start, "session_id", None)
            if not isinstance(session_id, str) or not session_id:
                raise RepairError("Dropbox upload session start returned an invalid result.")
            offset, recoveries = len(first), 0
            while True:
                stream.seek(offset)
                remaining = size - offset
                payload = stream.read(min(chunk_size, remaining))
                operation = "finish" if remaining <= chunk_size else "append"
                try:
                    if operation == "finish":
                        self._finish(session_id, offset, payload, destination_path)
                        return
                    self._call(
                        lambda payload=payload, offset=offset: self._client.files_upload_session_append_v2(
                            payload, UploadSessionCursor(session_id, offset)
                        ),
                        "upload session append",
                        passthrough_api=True,
                    )
                    offset += len(payload)
                except ApiError as exc:
                    corrected = self._correct_offset(exc.error)
                    if (
                        corrected is None
                        or recoveries >= 2
                        or corrected < 0
                        or corrected > size
                        or corrected == offset
                    ):
                        raise RepairError(
                            f"Dropbox upload session {operation} returned an unusable offset recovery."
                        ) from exc
                    offset, recoveries = corrected, recoveries + 1

    def _finish(self, session_id: str, offset: int, payload: bytes, path: str) -> Any:
        commit = CommitInfo(path=path, mode=WriteMode.overwrite, autorename=False)
        try:
            return self._call(
                lambda: self._client.files_upload_session_finish(
                    payload, UploadSessionCursor(session_id, offset), commit
                ),
                "upload session finish",
                passthrough_api=True,
            )
        except ApiError as exc:
            if self._is_finish_conflict(exc.error):
                raise RepairError("Dropbox found an existing destination file during repair.") from exc
            raise

    def _ensure_parents(self, path: str, root: str) -> None:
        parts = path.split("/")[1:-1]
        start = len(root.split("/")) if root else 1
        for index in range(start, len(parts) + 1):
            folder = "/" + "/".join(parts[:index])
            if folder in self._folders:
                continue
            try:
                self._call(
                    lambda folder=folder: self._client.files_create_folder_v2(
                        folder, autorename=False
                    ),
                    "parent-folder creation",
                    passthrough_api=True,
                )
            except ApiError as exc:
                error = exc.error
                if not (
                    isinstance(error, CreateFolderError)
                    and error.is_path()
                    and error.get_path().is_conflict()
                    and error.get_path().get_conflict().is_folder()
                ):
                    raise RepairError(
                        "Dropbox could not create a destination parent folder."
                    ) from exc
            self._folders.add(folder)

    def _call(self, action: Any, operation: str, *, passthrough_api: bool = False) -> Any:
        for attempt in range(4):
            try:
                return action()
            except (AuthError, BadInputError) as exc:
                raise RepairError(f"Dropbox {operation} authentication failed.") from exc
            except (RateLimitError, InternalServerError) as exc:
                if attempt == 3:
                    raise RepairError(f"Dropbox {operation} retry budget was exhausted.") from exc
                self._sleeper(2**attempt)
            except ApiError as exc:
                if passthrough_api:
                    raise
                raise RepairError(f"Dropbox {operation} failed.") from exc
        raise AssertionError("unreachable")

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


def normalize_root(value: object) -> str:
    if not isinstance(value, str) or (
        value and (not value.startswith("/") or "\\" in value or "//" in value)
    ):
        raise RepairError("root_path must be an absolute POSIX path.")
    if value and any(part in {"", ".", ".."} for part in value.split("/")[1:]):
        raise RepairError("root_path contains an invalid segment.")
    return value.rstrip("/")


def _required_credential(credentials: Mapping[str, Any], name: str, side: str) -> str:
    value = credentials.get(name)
    if not isinstance(value, str) or not value.strip():
        raise RepairError(f"{side} credentials require a non-empty {name}.")
    return value


def destination_path(root: str, relative_path: str) -> str:
    return f"{root}/{relative_path}" if root else f"/{relative_path}"


def _metadata_matches(metadata: FileMetadata, source: Mapping[str, Any]) -> bool:
    return (
        metadata.rev == source["rev"]
        and metadata.size == source["size"]
        and metadata.content_hash == source["content_hash"]
    )


def _is_under_root(path_lower: str | None, root: str, relative_path: str) -> bool:
    expected = f"{root}/{relative_path}" if root else f"/{relative_path}"
    return path_lower == expected.casefold()
