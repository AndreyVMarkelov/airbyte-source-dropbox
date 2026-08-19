from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)

from destination_dropbox.client import (
    DropboxAuthenticationError,
    DropboxClient,
    DropboxRateLimitError,
    DropboxWriteError,
)
from destination_dropbox.validation import (
    RecordValidationError,
    normalize_conflict_policy,
    normalize_root_path,
    validate_record,
)


class DestinationDropbox(Destination):
    """Write validated small files to Dropbox in configured Airbyte record order."""

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:
            normalize_root_path(config.get("root_path", ""))
            normalize_conflict_policy(config.get("conflict_policy", "overwrite"))
            client = DropboxClient(config)
            client.current_account()
            client.verify_root_path(config.get("root_path", ""))
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as exc:
            return AirbyteConnectionStatus(
                status=Status.FAILED, message=f"Unable to connect to Dropbox: {exc}"
            )

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        root_path = normalize_root_path(config.get("root_path", ""))
        conflict_policy = normalize_conflict_policy(config.get("conflict_policy", "overwrite"))
        max_file_size_bytes = int(config.get("max_file_size_mb", 10)) * 1024 * 1024
        configured_streams = {stream.stream.name for stream in configured_catalog.streams}
        record_index = 0
        client = DropboxClient(config)
        client.verify_root_path(root_path)

        for message in input_messages:
            if message.type == Type.STATE:
                # Reaching this message proves every preceding record was uploaded successfully.
                yield message
                continue
            if message.type != Type.RECORD:
                continue
            record_index += 1
            if message.record.stream not in configured_streams:
                raise RecordValidationError(
                    f"Record {record_index} from stream {message.record.stream!r} "
                    "is not in the configured catalog."
                )
            try:
                record = validate_record(
                    message.record.data,
                    root_path=root_path,
                    max_file_size_bytes=max_file_size_bytes,
                )
            except RecordValidationError as exc:
                raise RecordValidationError(
                    f"Invalid record {record_index} from stream {message.record.stream!r}: {exc}"
                ) from exc
            try:
                client.upload_file(record, conflict_policy, root_path)
            except (DropboxAuthenticationError, DropboxRateLimitError, DropboxWriteError) as exc:
                raise DropboxWriteError(
                    f"Failed to upload record {record_index} from stream "
                    f"{message.record.stream!r}: {exc}"
                ) from exc
