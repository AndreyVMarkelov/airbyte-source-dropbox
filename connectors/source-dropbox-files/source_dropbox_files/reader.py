from __future__ import annotations

import hashlib
import logging
import mimetypes
from collections.abc import Callable, Iterable
from pathlib import Path
from time import sleep
from typing import Any

import dropbox
from airbyte_cdk import FailureType
from airbyte_cdk.sources.file_based.file_based_stream_reader import (
    AbstractFileBasedStreamReader,
    FileReadMode,
)
from airbyte_cdk.sources.file_based.file_record_data import FileRecordData
from airbyte_cdk.sources.file_based.remote_file import RemoteFile, UploadableRemoteFile
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from dropbox.exceptions import (
    ApiError,
    AuthError,
    BadInputError,
    InternalServerError,
    RateLimitError,
)
from dropbox.files import FileMetadata
from pydantic.v1 import PrivateAttr

from source_dropbox_files.spec import SourceDropboxFilesSpec

MAX_RETRIES = 3


class DropboxFileSkipError(RuntimeError):
    """A content-specific error which safely skips one file."""


class DropboxFileRecordData(FileRecordData):
    file_id: str
    relative_path: str
    path_lower: str | None
    path_display: str | None
    rev: str | None
    content_hash: str | None
    sha256: str


class DropboxRemoteFile(UploadableRemoteFile):
    id: str
    size_bytes: int
    rev: str | None = None
    content_hash: str | None = None
    path_lower: str | None = None
    path_display: str | None = None
    client: Any
    chunk_size_bytes: int
    _downloaded_sha256: str | None = PrivateAttr(default=None)

    @property
    def size(self) -> int:
        return self.size_bytes

    @property
    def source_uri(self) -> str:
        return f"dropbox://{self.id}"

    @property
    def source_file_relative_path(self) -> str:
        return self.uri

    def download_to_local_directory(self, local_file_path: str) -> None:
        metadata, response = self.client.download(self.id)
        if not hasattr(response, "iter_content"):
            raise AirbyteTracedException(
                message="Dropbox returned an invalid file download response.",
                internal_message=(
                    "The Dropbox SDK download response does not support chunked reads."
                ),
                failure_type=FailureType.system_error,
            )
        if getattr(metadata, "rev", self.rev) != self.rev:
            raise DropboxFileSkipError("Dropbox file changed while it was being transferred.")

        bytes_written = 0
        digest = hashlib.sha256()
        try:
            with Path(local_file_path).open("wb") as output:
                for chunk in response.iter_content(chunk_size=self.chunk_size_bytes):
                    if not chunk:
                        continue
                    output.write(chunk)
                    digest.update(chunk)
                    bytes_written += len(chunk)
                output.flush()
        except OSError as exc:
            raise AirbyteTracedException(
                message="Airbyte staging storage could not write the Dropbox file.",
                internal_message="Failed while writing a native file-transfer staging file.",
                failure_type=FailureType.system_error,
            ) from exc
        if bytes_written != self.size:
            raise DropboxFileSkipError(
                "Downloaded byte count does not match Dropbox file metadata."
            )
        self._downloaded_sha256 = digest.hexdigest()

    @property
    def downloaded_sha256(self) -> str:
        value = self._downloaded_sha256
        if not isinstance(value, str):
            raise RuntimeError("Dropbox download completed without a SHA-256 digest.")
        return value


class SourceDropboxFilesStreamReader(AbstractFileBasedStreamReader):
    def __init__(self, *, sleeper: Callable[[float], None] = sleep) -> None:
        super().__init__()
        self._sleeper = sleeper
        self._client: Any = None

    @property
    def config(self) -> SourceDropboxFilesSpec:
        return self._config

    @config.setter
    def config(self, value: SourceDropboxFilesSpec) -> None:
        if not isinstance(value, SourceDropboxFilesSpec):
            raise TypeError("Expected SourceDropboxFilesSpec.")
        self._config = value
        credentials = value.credentials
        common_kwargs = {"max_retries_on_error": 0, "max_retries_on_rate_limit": 0}
        if credentials.auth_type == "oauth2_pkce":
            self._client = dropbox.Dropbox(
                oauth2_refresh_token=credentials.refresh_token,
                app_key=credentials.app_key,
                **common_kwargs,
            )
        else:
            self._client = dropbox.Dropbox(
                oauth2_access_token=credentials.access_token, **common_kwargs
            )

    def get_matching_files(
        self, globs: list[str], prefix: str | None, logger: logging.Logger
    ) -> Iterable[RemoteFile]:
        del prefix
        result = self._call("list", self._list_folder)
        while True:
            for entry in result.entries:
                if not isinstance(entry, FileMetadata):
                    continue
                remote_file = self._remote_file(entry)
                if remote_file.size > self._max_file_size_bytes:
                    logger.warning(
                        "Skipping Dropbox file %s because it exceeds the configured size limit.",
                        remote_file.id,
                    )
                    continue
                if self.file_matches_globs(remote_file, globs):
                    yield remote_file
            if not result.has_more:
                return
            cursor = result.cursor
            result = self._call(
                "list", lambda cursor=cursor: self._client.files_list_folder_continue(cursor)
            )

    def open_file(
        self, file: RemoteFile, mode: FileReadMode, encoding: str | None, logger: logging.Logger
    ) -> Any:
        del file, mode, encoding, logger
        raise RuntimeError("source-dropbox-files supports native raw-file transfer only.")

    def upload(
        self, file: UploadableRemoteFile, local_directory: str, logger: logging.Logger
    ) -> tuple[FileRecordData, Any]:
        record, reference = super().upload(file, local_directory, logger)
        assert isinstance(file, DropboxRemoteFile)
        return (
            DropboxFileRecordData(
                **record.dict(),
                file_id=file.id,
                relative_path=file.uri,
                path_lower=file.path_lower,
                path_display=file.path_display,
                rev=file.rev,
                content_hash=file.content_hash,
                sha256=file.downloaded_sha256,
            ),
            reference,
        )

    @property
    def _max_file_size_bytes(self) -> int:
        return self.config.file_transfer.max_file_size_mb * 1024 * 1024

    def download(self, file_id: str) -> tuple[Any, Any]:
        # A file can disappear after traversal but before its content request. That is
        # a per-file condition; list and staging failures remain connector failures.
        return self._call("download", lambda: self._client.files_download(file_id), file_level=True)

    def current_account(self) -> Any:
        return self._call("account check", self._client.users_get_current_account)

    def _list_folder(self) -> Any:
        return self._client.files_list_folder(
            self.config.path,
            recursive=self.config.recursive,
            include_deleted=False,
        )

    def _remote_file(self, entry: FileMetadata) -> DropboxRemoteFile:
        relative_path = self._relative_path(entry.path_display or entry.name)
        return DropboxRemoteFile(
            uri=relative_path,
            last_modified=entry.server_modified,
            id=entry.id,
            size_bytes=entry.size,
            rev=entry.rev,
            content_hash=entry.content_hash,
            path_lower=entry.path_lower,
            path_display=entry.path_display,
            mime_type=mimetypes.guess_type(entry.name)[0],
            created_at=entry.client_modified.isoformat() if entry.client_modified else None,
            updated_at=entry.server_modified.isoformat() if entry.server_modified else None,
            client=self,
            chunk_size_bytes=self.config.file_transfer.download_chunk_size_mb * 1024 * 1024,
        )

    def _relative_path(self, path_display: str) -> str:
        root = self.config.path.rstrip("/")
        if root and path_display.casefold().startswith(f"{root.casefold()}/"):
            return path_display[len(root) + 1 :]
        return path_display.lstrip("/")

    def _call(self, operation: str, action: Callable[[], Any], *, file_level: bool = False) -> Any:
        for retry in range(MAX_RETRIES + 1):
            try:
                return action()
            except (RateLimitError, InternalServerError) as exc:
                if retry == MAX_RETRIES:
                    raise AirbyteTracedException(
                        message=f"Dropbox {operation} was rate limited or unavailable.",
                        internal_message="Dropbox transient retry budget was exhausted.",
                        failure_type=FailureType.system_error,
                    ) from exc
                self._sleeper(min(2**retry, 30))
            except (AuthError, BadInputError) as exc:
                raise AirbyteTracedException(
                    message="Dropbox credentials are invalid or missing files.content.read.",
                    internal_message="Dropbox rejected the file-transfer credentials or scope.",
                    failure_type=FailureType.config_error,
                ) from exc
            except ApiError as exc:
                if file_level:
                    raise DropboxFileSkipError("Dropbox could not download this file.") from exc
                raise AirbyteTracedException(
                    message=f"Dropbox {operation} failed.",
                    internal_message=(
                        "Dropbox returned an unexpected API error while traversing files."
                    ),
                    failure_type=FailureType.system_error,
                ) from exc
        raise AssertionError("Unreachable Dropbox retry state.")
