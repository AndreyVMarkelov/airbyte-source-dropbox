from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterable
from pathlib import Path
from unittest.mock import Mock

import pytest
from airbyte_cdk.models import (
    AirbyteStateMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from dropbox.exceptions import ApiError
from dropbox.files import FileMetadata
from jsonschema import Draft7Validator

from source_dropbox.client import (
    DropboxClient,
    DropboxContentPermissionError,
    DropboxSharingPermissionError,
)
from source_dropbox.source import SourceDropbox

pytestmark = pytest.mark.integration


class _DiagnosticLogger:
    """Capture stream exceptions that Airbyte intentionally sanitizes in messages."""

    def __init__(self, secrets: Iterable[str]) -> None:
        self._secrets = tuple(secret for secret in secrets if secret)
        self.exceptions: list[str] = []

    def exception(self, message: str, *args: object, **kwargs: object) -> None:
        _, error, _ = sys.exc_info()
        detail = f"{message}: {type(error).__name__}: {error}" if error else message
        for secret in self._secrets:
            detail = detail.replace(secret, "[REDACTED]")
        self.exceptions.append(detail)

    def __getattr__(self, name: str) -> object:
        return lambda *args, **kwargs: None


def _catalog(
    config: dict[str, object], names: list[str], sync_mode: SyncMode = SyncMode.full_refresh
) -> ConfiguredAirbyteCatalog:
    streams = {stream.name: stream for stream in SourceDropbox().discover(Mock(), config).streams}
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name=name,
                    json_schema=streams[name].json_schema,
                    supported_sync_modes=streams[name].supported_sync_modes,
                ),
                sync_mode=sync_mode,
                destination_sync_mode=DestinationSyncMode.append,
                cursor_field=["cursor"] if name == "entries" else [],
            )
            for name in names
        ]
    )


def _read(
    config: dict[str, object],
    catalog: ConfiguredAirbyteCatalog,
    state: list[AirbyteStateMessage] | None = None,
) -> list[object]:
    messages: list[object] = []
    credentials = config.get("credentials", {})
    logger = _DiagnosticLogger(str(value) for value in credentials.values())
    try:
        for message in SourceDropbox().read(
            logger=logger, config=config, catalog=catalog, state=state
        ):
            messages.append(message)
    except AirbyteTracedException as exc:
        trace_errors = [
            message.trace.error.message
            for message in messages
            if message.type == Type.TRACE and message.trace and message.trace.error  # type: ignore[union-attr]
        ]
        detail = "; ".join(logger.exceptions) or "; ".join(trace_errors) or str(exc)
        pytest.fail(f"Airbyte protocol read failed: {detail}")
    return messages


def _records(messages: Iterable[object]) -> list[dict[str, object]]:
    return [message.record.data for message in messages if message.type == Type.RECORD]  # type: ignore[union-attr]


def _states(messages: Iterable[object]) -> list[AirbyteStateMessage]:
    return [message.state for message in messages if message.type == Type.STATE]  # type: ignore[union-attr]


def _schema(stream_name: str) -> dict[str, object]:
    schema_path = Path(__file__).parents[3] / "source_dropbox" / "schemas" / f"{stream_name}.json"
    return json.loads(schema_path.read_text())


def _assert_no_secret_leak(messages: Iterable[object], secrets: set[str]) -> None:
    rendered = "\n".join(str(message) for message in messages)
    assert not any(secret in rendered for secret in secrets)


@pytest.fixture(scope="session")
def integration_test_path(core_config: dict[str, object]) -> str:
    """Verify the read fixture before protocol tests mask its Dropbox error."""
    path = str(core_config["path"])
    try:
        DropboxClient(core_config).list_folder(path, recursive=True, include_deleted=False)
    except ApiError as exc:
        pytest.fail(
            f"Dropbox integration test path {path!r} is unavailable. "
            "Create it in the linked Dropbox account or set "
            "DROPBOX_INTEGRATION_TEST_PATH to an accessible folder. "
            f"Dropbox returned: {exc.error!s}"
        )
    return path


def test_live_spec_check_and_discover(core_config: dict[str, object]) -> None:
    source = SourceDropbox()
    spec = source.spec(Mock())
    ok, error = source.check_connection(Mock(), core_config)
    catalog = source.discover(Mock(), core_config)

    assert spec.connectionSpecification
    assert ok is True, error
    assert [stream.name for stream in catalog.streams] == [
        "entries",
        "files",
        "folders",
        "file_properties",
        "shared_links",
        "shared_folders",
        "sharing_acl",
        "file_contents",
    ]


def test_live_core_full_refresh_and_schema_validation(
    core_config: dict[str, object], integration_secrets: set[str], integration_test_path: str
) -> None:
    messages = _read(core_config, _catalog(core_config, ["files", "folders"]))
    records = _records(messages)

    for record in records:
        stream_name = "files" if "content_hash" in record else "folders"
        Draft7Validator(_schema(stream_name)).validate(record)
    _assert_no_secret_leak(messages, integration_secrets)


def test_live_entries_incremental_resume(
    core_config: dict[str, object], integration_secrets: set[str], integration_test_path: str
) -> None:
    catalog = _catalog(core_config, ["entries"], SyncMode.incremental)
    initial_messages = _read(core_config, catalog)
    state = _states(initial_messages)

    assert state, "Initial incremental sync must emit Dropbox cursor state."
    resumed_messages = _read(core_config, catalog, state)
    assert _states(resumed_messages), "Resumed incremental sync must emit valid cursor state."
    _assert_no_secret_leak([*initial_messages, *resumed_messages], integration_secrets)


def test_live_optional_scopes_remain_local(
    core_config: dict[str, object], integration_test_path: str
) -> None:
    client = DropboxClient(core_config)
    with pytest.raises(DropboxSharingPermissionError, match="sharing.read"):
        next(client.iter_shared_links())

    file_id = next(
        (
            entry.id
            for page in client.iter_entries(
                path=core_config["path"], recursive=True, include_deleted=False
            )
            for entry in page.entries
            if isinstance(entry, FileMetadata)
        ),
        None,
    )
    assert file_id, "The integration path must contain a file to verify content scope handling."
    with pytest.raises(DropboxContentPermissionError, match="files.content.read"):
        client.extract_markdown(file_id, 10)


def test_live_sharing_streams(sharing_config: dict[str, object]) -> None:
    messages = _read(sharing_config, _catalog(sharing_config, ["shared_links", "shared_folders"]))
    for record in _records(messages):
        stream_name = "shared_links" if "url" in record else "shared_folders"
        Draft7Validator(_schema(stream_name)).validate(record)


def test_live_file_contents(content_config: dict[str, object], integration_test_path: str) -> None:
    messages = _read(content_config, _catalog(content_config, ["file_contents"]))
    records = _records(messages)
    assert records, (
        "The integration path must contain a small .pdf or .docx file for Riviera live extraction."
    )
    for record in records:
        Draft7Validator(_schema("file_contents")).validate(record)
        assert record["extraction_status"] in {"succeeded", "failed", "timed_out"}


def test_live_invalid_refresh_token_is_safe_and_actionable(core_config: dict[str, object]) -> None:
    invalid_token = os.environ.get("DROPBOX_INTEGRATION_INVALID_REFRESH_TOKEN")
    if not invalid_token:
        pytest.skip("set DROPBOX_INTEGRATION_INVALID_REFRESH_TOKEN to run revoked-token coverage")
    config = {
        **core_config,
        "credentials": {**core_config["credentials"], "refresh_token": invalid_token},
    }

    ok, error = SourceDropbox().check_connection(Mock(), config)
    assert ok is False
    assert invalid_token not in str(error)
    assert "refresh token" in str(error).lower()


def test_live_pagination_fixture_spans_multiple_pages(core_config: dict[str, object]) -> None:
    pagination_path = os.environ.get("DROPBOX_INTEGRATION_PAGINATION_PATH")
    if not pagination_path:
        pytest.skip("set DROPBOX_INTEGRATION_PAGINATION_PATH to run multi-page listing coverage")
    pages = list(
        DropboxClient(core_config).iter_entries(
            path=pagination_path, recursive=True, include_deleted=False
        )
    )
    assert len(pages) > 1, "Pagination fixture must contain more than one Dropbox result page."
