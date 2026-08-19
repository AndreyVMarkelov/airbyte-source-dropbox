import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from airbyte_cdk.models import (
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)
from dropbox import async_, riviera
from dropbox.exceptions import ApiError, AuthError, BadInputError, RateLimitError
from dropbox.files import FileMetadata, FolderMetadata
from jsonschema import Draft7Validator

from source_dropbox.client import (
    DropboxAuthenticationError,
    DropboxClient,
    DropboxContentPermissionError,
    DropboxExtractionInfrastructureError,
    DropboxPage,
    DropboxRateLimitError,
    MarkdownExtraction,
)
from source_dropbox.source import SourceDropbox
from source_dropbox.streams.file_contents import FileContents

CONFIG = {
    "credentials": {"auth_type": "access_token", "access_token": "test-token"},
    "path": "/configured",
    "recursive": False,
    "include_deleted": True,
    "file_contents": {"allowed_extensions": [".PDF"], "max_file_size_mb": 1, "timeout_seconds": 10},
}


def _file(name: str = "report.PDF", size: int = 1024) -> FileMetadata:
    return FileMetadata(
        name=name,
        id="id:file",
        client_modified=datetime(2026, 8, 1, tzinfo=UTC),
        server_modified=datetime(2026, 8, 1, tzinfo=UTC),
        rev="0123456789",
        size=size,
        path_lower=f"/configured/{name.lower()}",
        path_display=f"/configured/{name}",
        content_hash="a" * 64,
        is_downloadable=True,
    )


def _schema() -> dict[str, object]:
    path = Path(__file__).parents[2] / "source_dropbox" / "schemas" / "file_contents.json"
    return json.loads(path.read_text())


def test_file_contents_filters_extensions_and_size_boundaries() -> None:
    client = Mock()
    limit = 1024 * 1024
    at_limit = _file(size=limit)
    over_limit = _file(name="large.pdf", size=limit + 1)
    other = _file(name="notes.txt")
    client.iter_entries.return_value = [
        DropboxPage(
            entries=[at_limit, over_limit, other, FolderMetadata(name="folder", id="id:folder")],
            cursor="page",
            has_more=False,
        )
    ]
    client.extract_markdown.return_value = MarkdownExtraction("# Report", "succeeded")

    records = list(FileContents(client, CONFIG).read_records(SyncMode.full_refresh))

    assert [record["name"] for record in records] == ["report.PDF"]
    assert records[0]["content_format"] == "markdown"
    client.extract_markdown.assert_called_once_with("id:file", 10)
    client.iter_entries.assert_called_once_with(
        path="/configured", recursive=False, include_deleted=False
    )
    assert list(Draft7Validator(_schema()).iter_errors(records[0])) == []


def test_file_contents_requires_allow_list_only_when_selected() -> None:
    client = Mock()
    config = {**CONFIG, "file_contents": {"allowed_extensions": []}}
    with pytest.raises(ValueError, match="allowed_extensions"):
        list(FileContents(client, config).read_records(SyncMode.full_refresh))


def test_file_contents_emits_file_level_error_record() -> None:
    client = Mock()
    client.iter_entries.return_value = [
        DropboxPage(entries=[_file()], cursor="page", has_more=False)
    ]
    client.extract_markdown.return_value = MarkdownExtraction(
        markdown=None,
        extraction_status="failed",
        error_type="unsupported_format_error",
        error_code="bad_request",
        error_details={"type": "unsupported_format_error"},
        error_message="Riviera extraction failed: unsupported_format_error.",
    )
    record = list(FileContents(client, CONFIG).read_records(SyncMode.full_refresh))[0]
    assert record["markdown"] is None
    assert record["error_details"] == {"type": "unsupported_format_error"}
    assert list(Draft7Validator(_schema()).iter_errors(record)) == []


def test_file_contents_reads_through_source_protocol() -> None:
    client = Mock()
    client.iter_entries.return_value = [
        DropboxPage(entries=[_file()], cursor="page", has_more=False)
    ]
    client.extract_markdown.return_value = MarkdownExtraction("# Report", "succeeded")
    catalog = ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="file_contents",
                    json_schema=_schema(),
                    supported_sync_modes=[SyncMode.full_refresh],
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
                primary_key=[["file_id"]],
            )
        ]
    )
    with patch("source_dropbox.source.DropboxClient", return_value=client):
        messages = list(SourceDropbox().read(Mock(), CONFIG, catalog))
    records = [message.record.data for message in messages if message.type == Type.RECORD]
    assert records == [
        {
            "file_id": "id:file",
            "rev": "0123456789",
            "content_hash": "a" * 64,
            "name": "report.PDF",
            "path_lower": "/configured/report.pdf",
            "path_display": "/configured/report.PDF",
            "size": 1024,
            "content_format": "markdown",
            "markdown": "# Report",
            "extraction_status": "succeeded",
            "error_type": None,
            "error_code": None,
            "error_details": None,
            "error_message": None,
        }
    ]


def _riviera_client(clock: Mock, sleeper: Mock) -> tuple[DropboxClient, Mock]:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    client._sleeper = sleeper
    client._monotonic_clock = clock
    client._client.riviera_get_markdown_async.return_value = (
        async_.LaunchResultBase.async_job_id("job")
    )
    return client, client._client


def test_extract_markdown_polls_with_injected_clock_and_sleeper() -> None:
    clock = Mock(side_effect=[0, 0, 0, 1, 1])
    sleeper = Mock()
    client, sdk = _riviera_client(clock, sleeper)
    sdk.riviera_get_markdown_async_check.side_effect = [
        riviera.GetMarkdownAsyncCheckResult.in_progress,
        riviera.GetMarkdownAsyncCheckResult.complete(
            riviera.GetMarkdownResult(markdown="# Report")
        ),
    ]

    assert client.extract_markdown("id:file", 10) == MarkdownExtraction("# Report", "succeeded")
    sleeper.assert_called_once_with(1.0)


def test_extract_markdown_timeout_does_not_overshoot_deadline() -> None:
    clock = Mock(side_effect=[0, 0, 0, 9.5, 9.5, 10])
    sleeper = Mock()
    client, sdk = _riviera_client(clock, sleeper)
    sdk.riviera_get_markdown_async_check.return_value = (
        riviera.GetMarkdownAsyncCheckResult.in_progress
    )

    extraction = client.extract_markdown("id:file", 10)
    assert extraction.extraction_status == "timed_out"
    assert extraction.error_type == "timeout"
    assert extraction.error_message == "Riviera extraction exceeded 10 seconds."
    assert [call.args[0] for call in sleeper.call_args_list] == [1.0, 0.5]


def test_extract_markdown_classifies_file_and_system_failures() -> None:
    clock = Mock(return_value=0)
    client, sdk = _riviera_client(clock, Mock())
    file_error = riviera.GetMarkdownAsyncError(
        error_code=riviera.ErrorCode("bad_request"),
        error_details=riviera.MarkdownConversionApiV2Error("unsupported_format_error"),
    )
    sdk.riviera_get_markdown_async_check.return_value = (
        riviera.GetMarkdownAsyncCheckResult.failed(file_error)
    )
    assert client.extract_markdown("id:file", 10).error_type == "unsupported_format_error"

    system_error = riviera.GetMarkdownAsyncError(
        error_code=riviera.ErrorCode("access_error")
    )
    sdk.riviera_get_markdown_async_check.return_value = (
        riviera.GetMarkdownAsyncCheckResult.failed(system_error)
    )
    with pytest.raises(DropboxExtractionInfrastructureError, match="access_error"):
        client.extract_markdown("id:file", 10)


def test_extract_markdown_classifies_content_scope_error() -> None:
    clock = Mock()
    client, sdk = _riviera_client(clock, Mock())
    sdk.riviera_get_markdown_async.side_effect = AuthError(
        "request-id", SimpleNamespace(_tag="missing_scope")
    )
    with pytest.raises(DropboxContentPermissionError, match="files.content.read"):
        client.extract_markdown("id:file", 10)


def test_extract_markdown_classifies_plaintext_content_scope_error() -> None:
    clock = Mock()
    client, sdk = _riviera_client(clock, Mock())
    sdk.riviera_get_markdown_async.side_effect = BadInputError(
        "request-id",
        "Your app is not permitted to access this endpoint because it does not have "
        "the required scope 'files.content.read'.",
    )

    with pytest.raises(DropboxContentPermissionError, match="files.content.read"):
        client.extract_markdown("id:file", 10)


def test_extract_markdown_classifies_refresh_token_failure_during_sync() -> None:
    clock = Mock()
    client, sdk = _riviera_client(clock, Mock())
    sdk.riviera_get_markdown_async.side_effect = BadInputError(
        "request-id", '{"error":"invalid_grant"}'
    )

    with pytest.raises(DropboxAuthenticationError, match="invalid or revoked"):
        client.extract_markdown("id:file", 10)


def test_markdown_poll_classifies_refresh_token_failure_during_sync() -> None:
    client, sdk = _riviera_client(Mock(), Mock())
    sdk.riviera_get_markdown_async_check.side_effect = BadInputError(
        "request-id", '{"error":"invalid_grant"}'
    )

    with pytest.raises(DropboxAuthenticationError, match="invalid or revoked"):
        client._check_markdown_job("job")


def test_extract_markdown_rate_limit_fails_stream() -> None:
    client, sdk = _riviera_client(Mock(), Mock())
    sdk.riviera_get_markdown_async.side_effect = RateLimitError("request-id")
    with pytest.raises(DropboxRateLimitError, match="content extraction"):
        client.extract_markdown("id:file", 10)


@pytest.mark.parametrize(
    "malformed_launch",
    [object(), SimpleNamespace(is_async_job_id=lambda: False)],
)
def test_extract_markdown_rejects_malformed_launch(malformed_launch: object) -> None:
    client, sdk = _riviera_client(Mock(), Mock())
    sdk.riviera_get_markdown_async.return_value = malformed_launch
    with pytest.raises(DropboxExtractionInfrastructureError, match="invalid extraction launch"):
        client.extract_markdown("id:file", 10)


def test_extract_markdown_rejects_malformed_check_response() -> None:
    client, sdk = _riviera_client(Mock(return_value=0), Mock())
    sdk.riviera_get_markdown_async_check.return_value = object()
    with pytest.raises(DropboxExtractionInfrastructureError, match="invalid extraction status"):
        client.extract_markdown("id:file", 10)


def test_extract_markdown_maps_other_status_to_failed_record() -> None:
    client, sdk = _riviera_client(Mock(return_value=0), Mock())
    sdk.riviera_get_markdown_async_check.return_value = riviera.GetMarkdownAsyncCheckResult("other")
    extraction = client.extract_markdown("id:file", 10)
    assert extraction.extraction_status == "failed"
    assert extraction.error_type == "unknown_status"


def test_extract_markdown_unexpected_api_error_fails_stream() -> None:
    client, sdk = _riviera_client(Mock(return_value=0), Mock())
    sdk.riviera_get_markdown_async_check.side_effect = ApiError(
        "request-id", Mock(), None, None
    )
    with pytest.raises(DropboxExtractionInfrastructureError, match="could not check"):
        client.extract_markdown("id:file", 10)
