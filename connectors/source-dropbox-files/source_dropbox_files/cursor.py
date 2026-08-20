from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping
from datetime import UTC, datetime
from typing import Any

from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.stream.cursor.abstract_file_based_cursor import (
    AbstractFileBasedCursor,
)

STATE_VERSION = 1


class DropboxFileVersionCursor(AbstractFileBasedCursor):
    """Tracks the successfully staged Dropbox byte version for each stable file ID."""

    def __init__(self, stream_config: FileBasedStreamConfig, **_: Any) -> None:
        super().__init__(stream_config)
        self._files: dict[str, dict[str, str]] = {}

    def set_initial_state(self, value: MutableMapping[str, Any]) -> None:
        if not value:
            self._files = {}
            return
        if value.get("version") != STATE_VERSION:
            raise ValueError("Dropbox file-transfer state has an unsupported version.")
        raw_files = value.get("files")
        if not isinstance(raw_files, Mapping):
            raise ValueError("Dropbox file-transfer state requires a files object.")
        parsed: dict[str, dict[str, str]] = {}
        for file_id, entry in raw_files.items():
            if not isinstance(file_id, str) or not file_id:
                raise ValueError("Dropbox file-transfer state contains an invalid file_id.")
            if not isinstance(entry, Mapping):
                raise ValueError(f"Dropbox file-transfer state for {file_id!r} must be an object.")
            parsed[file_id] = {
                field: _required_state_string(entry, field, file_id)
                for field in ("path", "rev", "content_hash")
            }
            _validate_relative_path(parsed[file_id]["path"], file_id)
        self._files = parsed

    def add_file(self, file: RemoteFile) -> None:
        file_id, version = _version_for(file)
        self._files[file_id] = version

    def get_state(self) -> MutableMapping[str, Any]:
        return {
            "version": STATE_VERSION,
            "files": {file_id: self._files[file_id] for file_id in sorted(self._files)},
        }

    def get_start_time(self) -> datetime:
        return datetime.min.replace(tzinfo=UTC)

    def get_files_to_sync(
        self, all_files: Iterable[RemoteFile], logger: Any
    ) -> Iterable[RemoteFile]:
        del logger
        for file in all_files:
            file_id, version = _version_for(file)
            previous = self._files.get(file_id)
            if previous is None or _bytes_changed(previous, version):
                yield file


def _version_for(file: RemoteFile) -> tuple[str, dict[str, str]]:
    file_id = getattr(file, "id", None)
    rev = getattr(file, "rev", None)
    content_hash = getattr(file, "content_hash", None)
    if not all(isinstance(value, str) and value for value in (file_id, rev, content_hash)):
        raise ValueError("Dropbox file metadata requires non-empty id, rev, and content_hash.")
    path = file.uri
    _validate_relative_path(path, file_id)
    return file_id, {"path": path, "rev": rev, "content_hash": content_hash}


def _bytes_changed(previous: Mapping[str, str], current: Mapping[str, str]) -> bool:
    return previous["rev"] != current["rev"] or previous["content_hash"] != current["content_hash"]


def _required_state_string(entry: Mapping[str, Any], field: str, file_id: str) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Dropbox file-transfer state for {file_id!r} requires non-empty {field}.")
    return value


def _validate_relative_path(path: str, file_id: str) -> None:
    if (
        path.startswith("/")
        or "\\" in path
        or "//" in path
        or any(segment in {"", ".", ".."} for segment in path.split("/"))
    ):
        raise ValueError(f"Dropbox file-transfer state for {file_id!r} contains an invalid path.")
