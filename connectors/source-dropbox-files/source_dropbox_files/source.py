from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from airbyte_cdk.models import ConfiguredAirbyteCatalog, Level, Type
from airbyte_cdk.sources.file_based.file_based_source import (
    FileBasedSource,
    preserve_directory_structure,
    use_file_transfer,
)
from airbyte_cdk.sources.file_based.stream.default_file_based_stream import DefaultFileBasedStream
from airbyte_cdk.sources.file_based.types import StreamSlice
from airbyte_cdk.sources.utils.record_helper import stream_data_to_airbyte_message
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from dropbox.files import DeletedMetadata, FileMetadata

from source_dropbox_files.cursor import DropboxFileVersionCursor, MigrationOperation
from source_dropbox_files.reader import DropboxCursorResetError, SourceDropboxFilesStreamReader
from source_dropbox_files.spec import SourceDropboxFilesSpec


class DropboxIncrementalFileTransferStream(DefaultFileBasedStream):
    """Native transfer stream whose Dropbox versions are tracked by stable file ID."""

    @property
    def supports_incremental(self) -> bool:
        return True

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
                "client_modified": {"type": ["null", "string"], "format": "date-time"},
                "server_modified": {"type": ["null", "string"], "format": "date-time"},
                "operation": {"type": "string", "enum": ["move", "delete"]},
                "old_path": {"type": "string"},
                "new_path": {"type": "string"},
                "old_content_hash": {"type": "string"},
                "new_content_hash": {"type": "string"},
            }
        )
        return schema

    def compute_slices(self) -> list[StreamSlice]:
        if not isinstance(self.cursor, DropboxFileVersionCursor):
            return list(super().compute_slices())
        if self.cursor.has_dropbox_cursor:
            assert self.cursor.dropbox_cursor is not None
            try:
                changed_files, deleted_paths = self._delta_changes(self.cursor.dropbox_cursor)
            except DropboxCursorResetError:
                self.logger.warning(
                    "Dropbox file-transfer cursor was reset; falling back to a full rescan."
                )
                return [self._snapshot_slice()]
            plan = self.cursor.plan_delta(
                changed_files,
                deleted_paths,
                rename_policy=self.stream_reader.config.rename_policy,
                delete_policy=self.stream_reader.config.delete_policy,
                path=self.stream_reader.config.path,
                recursive=self.stream_reader.config.recursive,
            )
            return [
                {
                    "operations": plan.operations,
                    self.FILES_KEY: plan.files,
                    "dropbox_cursor": self.stream_reader.last_cursor,
                }
            ]
        return [self._snapshot_slice()]

    def _snapshot_slice(self) -> StreamSlice:
        assert isinstance(self.cursor, DropboxFileVersionCursor)
        all_files, transfer_files = self.stream_reader.snapshot_files(
            self.config.globs or [], logger=self.logger
        )
        transfer_file_ids = {getattr(file, "id", None) for file in transfer_files}
        plan = self.cursor.plan_inventory(
            all_files,
            rename_policy=self.stream_reader.config.rename_policy,
            delete_policy=self.stream_reader.config.delete_policy,
            path=self.stream_reader.config.path,
            recursive=self.stream_reader.config.recursive,
        )
        files = [
            file for file in plan.files if getattr(file, "id", None) in transfer_file_ids
        ]
        return {
            "operations": plan.operations,
            self.FILES_KEY: files,
            "dropbox_cursor": self.stream_reader.last_cursor,
        }

    def read_records_from_slice(self, stream_slice: StreamSlice):
        operations = stream_slice.get("operations", [])
        for operation in operations:
            assert isinstance(operation, MigrationOperation)
            yield stream_data_to_airbyte_message(self.name, operation.record())
            if operation.kind == "move":
                assert isinstance(self.cursor, DropboxFileVersionCursor)
                self.cursor.mark_move(operation)
            else:
                assert isinstance(self.cursor, DropboxFileVersionCursor)
                self.cursor.mark_delete(operation)
        saw_error = False
        for message in super().read_records_from_slice(
            {self.FILES_KEY: stream_slice[self.FILES_KEY]}
        ):
            if _is_error_log(message):
                saw_error = True
            yield message
        if not saw_error:
            dropbox_cursor = stream_slice.get("dropbox_cursor")
            if isinstance(dropbox_cursor, str) and dropbox_cursor:
                assert isinstance(self.cursor, DropboxFileVersionCursor)
                self.cursor.advance_cursor(dropbox_cursor)

    def _delta_changes(self, cursor: str) -> tuple[list[Any], list[str]]:
        changed_files = []
        deleted_paths = []
        for entry in self.stream_reader.iter_delta_entries(cursor):
            if isinstance(entry, FileMetadata):
                remote_file = self.stream_reader._remote_file(entry)  # noqa: SLF001
                if remote_file.size > self.stream_reader._max_file_size_bytes:  # noqa: SLF001
                    self.logger.warning(
                        "Skipping Dropbox file %s because it exceeds the configured size limit.",
                        remote_file.id,
                    )
                    continue
                if self.stream_reader.file_matches_globs(
                    remote_file, self.config.globs or []
                ):
                    changed_files.append(remote_file)
            elif isinstance(entry, DeletedMetadata):
                path = entry.path_display or entry.path_lower
                if isinstance(path, str) and path:
                    deleted_paths.append(self.stream_reader._relative_path(path))  # noqa: SLF001
        return changed_files, deleted_paths


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
            cursor_cls=DropboxFileVersionCursor,
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
        return DropboxIncrementalFileTransferStream(
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


def _is_error_log(message: Any) -> bool:
    return (
        getattr(message, "type", None) == Type.LOG
        and getattr(getattr(message, "log", None), "level", None) == Level.ERROR
    )
