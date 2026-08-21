from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import CheckpointMixin
from airbyte_cdk.sources.streams.checkpoint import CheckpointMode
from dropbox.files import FileMetadata

from source_dropbox.client import MarkdownExtraction
from source_dropbox.streams.base import DropboxStream

RIVIERA_MARKDOWN_EXTENSIONS = frozenset(
    {".binder", ".docx", ".html", ".paper", ".papert", ".pptx", ".xlsx", ".gsheet", ".ods", ".pdf"}
)
RIVIERA_MAX_FILE_SIZE_MB = 50
STATE_VERSION = 1


@dataclass(frozen=True)
class FileContentsSettings:
    allowed_extensions: frozenset[str]
    max_file_size_mb: int
    timeout_seconds: int

    @property
    def max_bytes(self) -> int:
        return self.max_file_size_mb * 1024 * 1024


@dataclass(frozen=True)
class FileVersion:
    rev: str
    content_hash: str
    path: str | None


class FileContents(DropboxStream, CheckpointMixin):
    """Opt-in Markdown extraction for eligible Dropbox documents."""

    name = "file_contents"
    primary_key = "file_id"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._state: dict[str, Any] = {"version": STATE_VERSION, "files": {}}
        self._pending_files: dict[str, FileMetadata] = {}

    @property
    def cursor_field(self) -> list[str]:
        # Version state is connector state only. There is no public record field
        # that represents this state, so discovery must not advertise a synthetic
        # cursor field.
        return []

    @property
    def supports_incremental(self) -> bool:
        return True

    @property
    def _checkpoint_mode(self) -> CheckpointMode:
        # CDK 6.x treats an incremental stream with an empty cursor_field as
        # resumable full refresh by default. file_contents deliberately has no
        # public record cursor, but it does have stream-managed incremental state.
        return CheckpointMode.INCREMENTAL

    @property
    def state(self) -> MutableMapping[str, Any]:
        return {
            "version": STATE_VERSION,
            "files": {
                file_id: self._state["files"][file_id]
                for file_id in sorted(self._state["files"])
            },
        }

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._state = _validate_state(value or {})

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        settings = self._settings()
        if not settings.allowed_extensions:
            raise ValueError(
                "file_contents requires file_contents.allowed_extensions "
                "when the stream is selected."
            )

        for page in self.client.iter_entries(
            path=self.config.get("path", ""),
            recursive=self.config.get("recursive", True),
            include_deleted=False,
        ):
            for entry in page.entries:
                if not self._eligible(entry, settings):
                    continue
                file_version = _version_for(entry)
                if sync_mode == SyncMode.incremental and self._is_unchanged(
                    entry.id, file_version
                ):
                    continue
                slice_id = f"{entry.id}:{entry.rev}:{entry.content_hash}"
                self._pending_files[slice_id] = entry
                yield {"file": slice_id}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        if not stream_slice:
            return

        slice_id = stream_slice["file"]
        entry = self._pending_files[slice_id]
        settings = self._settings()
        try:
            extraction = self.client.extract_markdown(entry.id, settings.timeout_seconds)
            yield self._record(entry, extraction)
            if extraction.extraction_status in {"succeeded", "failed"}:
                self._state["files"][entry.id] = _version_for(entry).__dict__
        finally:
            self._pending_files.pop(slice_id, None)

    def _is_unchanged(self, file_id: str, version: FileVersion) -> bool:
        prior = self._state["files"].get(file_id)
        return (
            isinstance(prior, dict)
            and prior.get("rev") == version.rev
            and prior.get("content_hash") == version.content_hash
        )

    def _settings(self) -> FileContentsSettings:
        raw = self.config.get("file_contents", {})
        extensions = raw.get("allowed_extensions", [])
        if not isinstance(extensions, list) or not all(
            isinstance(value, str) for value in extensions
        ):
            raise ValueError("file_contents.allowed_extensions must be an array of extensions.")
        normalized_extensions = frozenset(value.lower() for value in extensions)
        unsupported = normalized_extensions - RIVIERA_MARKDOWN_EXTENSIONS
        if unsupported:
            values = ", ".join(sorted(unsupported))
            raise ValueError(f"Unsupported file_contents extension(s): {values}.")

        max_file_size_mb = raw.get("max_file_size_mb", 10)
        timeout_seconds = raw.get("timeout_seconds", 300)
        if not isinstance(max_file_size_mb, int) or not (
            1 <= max_file_size_mb <= RIVIERA_MAX_FILE_SIZE_MB
        ):
            raise ValueError("file_contents.max_file_size_mb must be between 1 and 50.")
        if not isinstance(timeout_seconds, int) or not 10 <= timeout_seconds <= 600:
            raise ValueError("file_contents.timeout_seconds must be between 10 and 600.")
        return FileContentsSettings(normalized_extensions, max_file_size_mb, timeout_seconds)

    @staticmethod
    def _eligible(entry: object, settings: FileContentsSettings) -> bool:
        return (
            isinstance(entry, FileMetadata)
            and Path(entry.name).suffix.lower() in settings.allowed_extensions
            and entry.size <= settings.max_bytes
        )

    @staticmethod
    def _record(entry: FileMetadata, extraction: MarkdownExtraction) -> dict[str, Any]:
        return {
            "file_id": entry.id,
            "rev": entry.rev,
            "content_hash": entry.content_hash,
            "name": entry.name,
            "path_lower": entry.path_lower,
            "path_display": entry.path_display,
            "size": entry.size,
            "client_modified": _isoformat_utc(entry.client_modified),
            "server_modified": _isoformat_utc(entry.server_modified),
            "content_format": "markdown",
            "markdown": extraction.markdown,
            "extraction_status": extraction.extraction_status,
            "error_type": extraction.error_type,
            "error_code": extraction.error_code,
            "error_details": extraction.error_details,
            "error_message": extraction.error_message,
        }


def _version_for(entry: FileMetadata) -> FileVersion:
    if not isinstance(entry.id, str) or not entry.id:
        raise ValueError("file_contents encountered a file without a stable Dropbox file ID.")
    if not isinstance(entry.rev, str) or not entry.rev:
        raise ValueError("file_contents encountered a file without a Dropbox rev.")
    if not isinstance(entry.content_hash, str) or not entry.content_hash:
        raise ValueError("file_contents encountered a file without a Dropbox content_hash.")
    return FileVersion(rev=entry.rev, content_hash=entry.content_hash, path=entry.path_lower)


def _validate_state(value: Mapping[str, Any]) -> dict[str, Any]:
    if not value:
        return {"version": STATE_VERSION, "files": {}}
    if value.get("version") != STATE_VERSION:
        raise ValueError("file_contents state has an unsupported version.")
    files = value.get("files")
    if not isinstance(files, Mapping):
        raise ValueError("file_contents state must contain a files object.")
    normalized: dict[str, dict[str, str | None]] = {}
    for file_id, version in files.items():
        if not isinstance(file_id, str) or not file_id:
            raise ValueError("file_contents state contains an invalid file_id.")
        if not isinstance(version, Mapping):
            raise ValueError("file_contents state file versions must be objects.")
        rev = version.get("rev")
        content_hash = version.get("content_hash")
        path = version.get("path")
        if not isinstance(rev, str) or not rev:
            raise ValueError("file_contents state file version is missing rev.")
        if not isinstance(content_hash, str) or not content_hash:
            raise ValueError("file_contents state file version is missing content_hash.")
        if path is not None and not isinstance(path, str):
            raise ValueError("file_contents state path must be a string or null.")
        normalized[file_id] = {"rev": rev, "content_hash": content_hash, "path": path}
    return {
        "version": STATE_VERSION,
        "files": {file_id: normalized[file_id] for file_id in sorted(normalized)},
    }


def _isoformat_utc(value: object) -> str | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
