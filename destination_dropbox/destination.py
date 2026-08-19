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

from destination_dropbox.client import DropboxClient
from destination_dropbox.validation import (
    RecordValidationError,
    normalize_root_path,
    validate_record,
)


class DestinationDropbox(Destination):
    """Validate Dropbox file-write records; uploads are intentionally deferred."""

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:
            normalize_root_path(config.get("root_path", ""))
            DropboxClient(config).current_account()
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
        max_file_size_bytes = int(config.get("max_file_size_mb", 10)) * 1024 * 1024
        configured_streams = {stream.stream.name for stream in configured_catalog.streams}
        record_index = 0

        # TODO(upload PR): buffer/flush records, then forward each STATE message only after
        # every preceding record has been durably uploaded. Do not blindly pass state through.
        for message in input_messages:
            if message.type != Type.RECORD:
                continue
            record_index += 1
            if message.record.stream not in configured_streams:
                raise RecordValidationError(
                    f"Record {record_index} from stream {message.record.stream!r} "
                    "is not in the configured catalog."
                )
            try:
                validate_record(
                    message.record.data,
                    root_path=root_path,
                    max_file_size_bytes=max_file_size_bytes,
                )
            except RecordValidationError as exc:
                raise RecordValidationError(
                    f"Invalid record {record_index} from stream {message.record.stream!r}: {exc}"
                ) from exc

        return ()
