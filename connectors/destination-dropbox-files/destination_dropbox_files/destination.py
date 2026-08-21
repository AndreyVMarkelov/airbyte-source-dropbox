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

from destination_dropbox_files.client import DropboxFilesClient, DropboxFilesWriteError
from destination_dropbox_files.validation import (
    FileReferenceValidationError,
    normalize_metadata_policy,
    normalize_root_path,
    validate_propagation_operation,
    validate_staged_file,
    verify_sha256,
)


class DestinationDropboxFiles(Destination):
    """Consume native Airbyte staging references and stream them into Dropbox sessions."""

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:
            root = normalize_root_path(config.get("root_path", ""))
            normalize_metadata_policy(config.get("metadata_policy", "preserve"))
            client = DropboxFilesClient(config)
            client.current_account()
            client.verify_root_path(root)
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as exc:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"Unable to connect to Dropbox: {exc}")

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        root = normalize_root_path(config.get("root_path", ""))
        policy = config.get("conflict_policy", "overwrite")
        if policy not in {"overwrite", "fail"}:
            raise FileReferenceValidationError("conflict_policy must be overwrite or fail.")
        metadata_policy = normalize_metadata_policy(config.get("metadata_policy", "preserve"))
        streams = {stream.stream.name for stream in configured_catalog.streams}
        client = DropboxFilesClient(config)
        client.verify_root_path(root)
        index = 0
        for message in input_messages:
            if message.type == Type.STATE:
                # All preceding staged files have fully committed before state passes through.
                yield message
                continue
            if message.type != Type.RECORD or message.record is None:
                continue
            index += 1
            if message.record.stream not in streams:
                raise FileReferenceValidationError(
                    f"Record {index} from stream {message.record.stream!r} is not configured."
                )
            operation = message.record.data.get("operation")
            if operation is not None:
                try:
                    client.apply_propagation(
                        validate_propagation_operation(message.record.data), root
                    )
                except (FileReferenceValidationError, DropboxFilesWriteError) as exc:
                    raise DropboxFilesWriteError(
                        f"Failed to apply propagation operation {index} from stream {message.record.stream!r}: {exc}"
                    ) from exc
                continue
            reference = message.record.file_reference
            if reference is None:
                raise FileReferenceValidationError(
                    f"Record {index} from stream {message.record.stream!r} is not a native file reference."
                )
            try:
                staged = validate_staged_file(
                    staging_file_url=reference.staging_file_url,
                    relative_path=reference.source_file_relative_path or message.record.data.get("relative_path"),
                    file_size_bytes=reference.file_size_bytes,
                    root_path=root,
                    sha256=message.record.data.get("sha256"),
                    client_modified=message.record.data.get("client_modified"),
                    metadata_policy=metadata_policy,
                )
                verify_sha256(staged)
                client.upload_staged_file(staged, root, policy)
            except (FileReferenceValidationError, DropboxFilesWriteError) as exc:
                raise DropboxFilesWriteError(
                    f"Failed to write referenced file {index} from stream {message.record.stream!r}: {exc}"
                ) from exc
