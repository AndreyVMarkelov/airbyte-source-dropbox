from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from airbyte_cdk.models import ConfiguredAirbyteCatalog
from airbyte_cdk.sources.file_based.file_based_source import (
    FileBasedSource,
    preserve_directory_structure,
    use_file_transfer,
)
from airbyte_cdk.sources.file_based.stream.cursor.default_file_based_cursor import (
    DefaultFileBasedCursor,
)
from airbyte_cdk.sources.file_based.stream.default_file_based_stream import DefaultFileBasedStream
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

from source_dropbox_files.reader import SourceDropboxFilesStreamReader
from source_dropbox_files.spec import SourceDropboxFilesSpec


class DropboxSnapshotFileTransferStream(DefaultFileBasedStream):
    """Raw files are intentionally snapshot-only; no Dropbox cursor is reused."""

    @property
    def supports_incremental(self) -> bool:
        return False

    @property
    def primary_key(self) -> list[str]:
        """Dropbox file IDs are stable across rename and are the transfer identity."""
        return ["file_id"]

    def get_json_schema(self) -> dict[str, Any]:
        schema = super().get_json_schema()
        schema["properties"].update(
            {
                "file_id": {"type": "string"},
                "relative_path": {"type": "string"},
                "path_lower": {"type": ["null", "string"]},
                "path_display": {"type": ["null", "string"]},
                "rev": {"type": ["null", "string"]},
                "content_hash": {"type": ["null", "string"]},
                "sha256": {"type": "string"},
            }
        )
        return schema


class SourceDropboxFiles(FileBasedSource):
    """Native Airbyte File Transfer source for original Dropbox bytes."""

    _concurrency_level = 1

    def __init__(
        self,
        catalog: ConfiguredAirbyteCatalog | None,
        config: Mapping[str, Any] | None,
        state: list[Any] | None,
    ) -> None:
        super().__init__(
            stream_reader=SourceDropboxFilesStreamReader(),
            spec_class=SourceDropboxFilesSpec,
            catalog=catalog,
            config=config,
            state=state,
            cursor_cls=DefaultFileBasedCursor,
        )

    def check_connection(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> tuple[bool, Any | None]:
        """Validate Dropbox credentials before native availability checks traverse the root."""
        try:
            self.stream_reader.config = self._get_parsed_config(config)
            self.stream_reader.current_account()
        except AirbyteTracedException as exc:
            return False, exc.message
        return super().check_connection(logger, config)

    def _make_default_stream(self, *args: Any, **kwargs: Any) -> DefaultFileBasedStream:
        stream_config, cursor, parsed_config = args
        return DropboxSnapshotFileTransferStream(
            config=stream_config,
            catalog_schema=self.stream_schemas.get(stream_config.name),
            stream_reader=self.stream_reader,
            availability_strategy=self.availability_strategy,
            discovery_policy=self.discovery_policy,
            parsers=self.parsers,
            validation_policy=self._validate_and_get_validation_policy(stream_config),
            errors_collector=self.errors_collector,
            cursor=cursor,
            use_file_transfer=use_file_transfer(parsed_config),
            preserve_directory_structure=preserve_directory_structure(parsed_config),
        )
