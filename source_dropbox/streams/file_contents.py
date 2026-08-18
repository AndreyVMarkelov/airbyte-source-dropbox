from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from airbyte_cdk.models import SyncMode
from dropbox.files import FileMetadata

from source_dropbox.client import MarkdownExtraction
from source_dropbox.streams.base import DropboxStream

RIVIERA_MARKDOWN_EXTENSIONS = frozenset(
    {".binder", ".docx", ".html", ".paper", ".papert", ".pptx", ".xlsx", ".gsheet", ".ods", ".pdf"}
)
RIVIERA_MAX_FILE_SIZE_MB = 50


@dataclass(frozen=True)
class FileContentsSettings:
    allowed_extensions: frozenset[str]
    max_file_size_mb: int
    timeout_seconds: int

    @property
    def max_bytes(self) -> int:
        return self.max_file_size_mb * 1024 * 1024


class FileContents(DropboxStream):
    """Opt-in full-refresh Markdown extraction for eligible Dropbox documents."""

    name = "file_contents"
    primary_key = "file_id"

    @property
    def supports_incremental(self) -> bool:
        return False

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
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
                extraction = self.client.extract_markdown(entry.id, settings.timeout_seconds)
                yield self._record(entry, extraction)

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
            "content_format": "markdown",
            "markdown": extraction.markdown,
            "extraction_status": extraction.extraction_status,
            "error_type": extraction.error_type,
            "error_code": extraction.error_code,
            "error_details": extraction.error_details,
            "error_message": extraction.error_message,
        }
