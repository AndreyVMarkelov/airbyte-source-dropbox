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


def _file(
    name: str = "report.PDF",
    size: int = 1024,
    *,
    file_id: str = "id:file",
    rev: str = "0123456789",
    content_hash: str = "a" * 64,
    path_lower: str | None = None,
    path_display: str | None = None,
) -> FileMetadata:
    return FileMetadata(
        name=name,
        id=file_id,
        client_modified=datetime(2026, 8, 1, tzinfo=UTC),
        server_modified=datetime(2026, 8, 1, 1, tzinfo=UTC),
        rev=rev,
        size=size,
        path_lower=path_lower if path_lower is not None else f"/configured/{name.lower()}",
        path_display=path_display if path_display is not None else f"/configured/{name}",
        content_hash=content_hash,
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

    records = _consume_file_contents(FileContents(client, CONFIG), SyncMode.full_refresh)

    assert [record["name"] for record in records] == ["report.PDF"]
    assert records[0]["content_format"] == "markdown"
    assert records[0]["client_modified"] == "2026-08-01T00:00:00Z"
    assert records[0]["server_modified"] == "2026-08-01T01:00:00Z"
    client.extract_markdown.assert_called_once_with("id:file", 10, namespace_id=None)
    client.iter_entries.assert_called_once_with(
        path="/configured", recursive=False, include_deleted=False
    )
    assert list(Draft7Validator(_schema()).iter_errors(records[0])) == []


def test_file_contents_requires_allow_list_only_when_selected() -> None:
    client = Mock()
    config = {**CONFIG, "file_contents": {"allowed_extensions": []}}
    with pytest.raises(ValueError, match="allowed_extensions"):
        list(FileContents(client, config).stream_slices(sync_mode=SyncMode.full_refresh))


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
    record = _consume_file_contents(FileContents(client, CONFIG), SyncMode.full_refresh)[0]
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
            "client_modified": "2026-08-01T00:00:00Z",
            "server_modified": "2026-08-01T01:00:00Z",
            "content_format": "markdown",
            "markdown": "# Report",
            "extraction_status": "succeeded",
            "error_type": None,
            "error_code": None,
            "error_details": None,
            "error_message": None,
        }
    ]


def _consume_file_contents(stream: FileContents, sync_mode: SyncMode) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for stream_slice in stream.stream_slices(sync_mode=sync_mode):
        records.extend(
            stream.read_records(sync_mode=sync_mode, stream_slice=stream_slice)
        )
    return records


def test_file_contents_incremental_first_run_extracts_and_updates_state() -> None:
    client = Mock()
    client.iter_entries.return_value = [
        DropboxPage(entries=[_file()], cursor="page", has_more=False)
    ]
    client.extract_markdown.return_value = MarkdownExtraction("# Report", "succeeded")
    stream = FileContents(client, CONFIG)

    records = _consume_file_contents(stream, SyncMode.incremental)

    assert [record["file_id"] for record in records] == ["id:file"]
    client.extract_markdown.assert_called_once_with("id:file", 10, namespace_id=None)
    assert stream.state == {
        "version": 1,
        "files": {
            "id:file": {
                "rev": "0123456789",
                "content_hash": "a" * 64,
                "path": "/configured/report.pdf",
            }
        },
    }


def test_file_contents_incremental_skips_unchanged_and_rename_only_files() -> None:
    client = Mock()
    renamed = _file(path_lower="/configured/renamed.pdf", path_display="/configured/renamed.PDF")
    client.iter_entries.return_value = [
        DropboxPage(entries=[renamed], cursor="page", has_more=False)
    ]
    stream = FileContents(client, CONFIG)
    stream.state = {
        "version": 1,
        "files": {
            "id:file": {
                "rev": "0123456789",
                "content_hash": "a" * 64,
                "path": "/configured/report.pdf",
            }
        },
    }

    assert _consume_file_contents(stream, SyncMode.incremental) == []
    client.extract_markdown.assert_not_called()
    assert stream.state["files"]["id:file"]["path"] == "/configured/report.pdf"


@pytest.mark.parametrize(
    ("rev", "content_hash"),
    [("1234567890", "a" * 64), ("0123456789", "b" * 64)],
)
def test_file_contents_incremental_extracts_changed_bytes(
    rev: str, content_hash: str
) -> None:
    client = Mock()
    client.iter_entries.return_value = [
        DropboxPage(
            entries=[_file(rev=rev, content_hash=content_hash)],
            cursor="page",
            has_more=False,
        )
    ]
    client.extract_markdown.return_value = MarkdownExtraction("# Updated", "succeeded")
    stream = FileContents(client, CONFIG)
    stream.state = {
        "version": 1,
        "files": {
            "id:file": {
                "rev": "0123456789",
                "content_hash": "a" * 64,
                "path": "/configured/report.pdf",
            }
        },
    }

    records = _consume_file_contents(stream, SyncMode.incremental)

    assert records[0]["markdown"] == "# Updated"
    assert stream.state["files"]["id:file"] == {
        "rev": rev,
        "content_hash": content_hash,
        "path": "/configured/report.pdf",
    }


def test_file_contents_incremental_extracts_new_file() -> None:
    client = Mock()
    client.iter_entries.return_value = [
        DropboxPage(
            entries=[_file(file_id="id:new", path_lower="/configured/new.pdf")],
            cursor="page",
            has_more=False,
        )
    ]
    client.extract_markdown.return_value = MarkdownExtraction("# New", "succeeded")
    stream = FileContents(client, CONFIG)

    records = _consume_file_contents(stream, SyncMode.incremental)

    assert records[0]["file_id"] == "id:new"
    assert "id:new" in stream.state["files"]


def test_file_contents_full_refresh_ignores_existing_state() -> None:
    client = Mock()
    client.iter_entries.return_value = [
        DropboxPage(entries=[_file()], cursor="page", has_more=False)
    ]
    client.extract_markdown.return_value = MarkdownExtraction("# Report", "succeeded")
    stream = FileContents(client, CONFIG)
    stream.state = {
        "version": 1,
        "files": {
            "id:file": {
                "rev": "0123456789",
                "content_hash": "a" * 64,
                "path": "/configured/report.pdf",
            }
        },
    }

    assert len(_consume_file_contents(stream, SyncMode.full_refresh)) == 1
    client.extract_markdown.assert_called_once()


def test_file_contents_failure_record_advances_state_but_infrastructure_failure_does_not() -> None:
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
    stream = FileContents(client, CONFIG)

    records = _consume_file_contents(stream, SyncMode.incremental)

    assert records[0]["extraction_status"] == "failed"
    assert "id:file" in stream.state["files"]

    failing_client = Mock()
    failing_client.iter_entries.return_value = [
        DropboxPage(entries=[_file()], cursor="page", has_more=False)
    ]
    failing_client.extract_markdown.side_effect = RuntimeError("temporary Riviera outage")
    failing_stream = FileContents(failing_client, CONFIG)

    with pytest.raises(RuntimeError, match="temporary Riviera outage"):
        _consume_file_contents(failing_stream, SyncMode.incremental)
    assert failing_stream.state == {"version": 1, "files": {}}


def test_file_contents_timeout_record_does_not_advance_state_and_retries_next_run() -> None:
    client = Mock()
    client.iter_entries.return_value = [
        DropboxPage(entries=[_file()], cursor="page", has_more=False)
    ]
    client.extract_markdown.return_value = MarkdownExtraction(
        markdown=None,
        extraction_status="timed_out",
        error_type="timeout",
        error_message="Riviera extraction exceeded 10 seconds.",
    )
    stream = FileContents(client, CONFIG)

    records = _consume_file_contents(stream, SyncMode.incremental)

    assert records[0]["extraction_status"] == "timed_out"
    assert stream.state == {"version": 1, "files": {}}

    retry_client = Mock()
    retry_client.iter_entries.return_value = [
        DropboxPage(entries=[_file()], cursor="page", has_more=False)
    ]
    retry_client.extract_markdown.return_value = MarkdownExtraction("# Retry", "succeeded")
    retry_stream = FileContents(retry_client, CONFIG)
    retry_stream.state = stream.state

    retry_records = _consume_file_contents(retry_stream, SyncMode.incremental)

    assert retry_records[0]["markdown"] == "# Retry"
    retry_client.extract_markdown.assert_called_once_with("id:file", 10, namespace_id=None)


def test_file_contents_state_validation_and_deterministic_serialization() -> None:
    stream = FileContents(Mock(), CONFIG)
    stream.state = {
        "version": 1,
        "files": {
            "id:z": {"rev": "z", "content_hash": "z", "path": None},
            "id:a": {"rev": "a", "content_hash": "a", "path": "/a.pdf"},
        },
    }

    assert list(stream.state["files"]) == ["id:a", "id:z"]

    for bad_state in [
        {"version": 2, "files": {}},
        {"version": 1, "files": []},
        {"version": 1, "files": {"": {"rev": "r", "content_hash": "h"}}},
        {"version": 1, "files": {"id:file": {"content_hash": "h"}}},
        {"version": 1, "files": {"id:file": {"rev": "r"}}},
        {"version": 1, "files": {"id:file": {"rev": "r", "content_hash": "h", "path": 1}}},
    ]:
        with pytest.raises(ValueError, match="file_contents state"):
            stream.state = bad_state


def test_file_contents_state_binds_non_default_dropbox_context() -> None:
    config = {
        **CONFIG,
        "path_root": {"mode": "namespace_id", "namespace_id": "123"},
    }
    client = Mock()
    client.context_scope.return_value = {
        "team_mode": "none",
        "path_root_mode": "namespace_id",
        "namespace_id": "123",
    }
    stream = FileContents(client, config)
    stream.state = {
        "version": 1,
        "context": {"team_mode": "none", "path_root_mode": "namespace_id", "namespace_id": "123"},
        "files": {"id:file": {"rev": "r", "content_hash": "h", "path": "/a.pdf"}},
    }

    assert stream.state["context"] == {
        "team_mode": "none",
        "path_root_mode": "namespace_id",
        "namespace_id": "123",
    }


def test_file_contents_state_rejects_missing_context_for_non_default_dropbox_context() -> None:
    config = {
        **CONFIG,
        "path_root": {"mode": "namespace_id", "namespace_id": "123"},
    }
    client = Mock()
    client.context_scope.return_value = {
        "team_mode": "none",
        "path_root_mode": "namespace_id",
        "namespace_id": "123",
    }
    stream = FileContents(client, config)

    with pytest.raises(ValueError, match="team/path root context"):
        stream.state = {
            "version": 1,
            "files": {"id:file": {"rev": "r", "content_hash": "h", "path": "/a.pdf"}},
        }


def test_file_contents_state_rejects_changed_resolved_root_namespace() -> None:
    client = Mock()
    client.context_scope.return_value = {
        "team_mode": "user",
        "selected_member_id": "dbmid:member",
        "path_root_mode": "root",
        "namespace_id": "222",
    }
    stream = FileContents(client, CONFIG)

    with pytest.raises(ValueError, match="context does not match"):
        stream.state = {
            "version": 1,
            "context": {
                "team_mode": "user",
                "selected_member_id": "dbmid:member",
                "path_root_mode": "root",
                "namespace_id": "111",
            },
            "files": {"id:file": {"rev": "r", "content_hash": "h", "path": "/a.pdf"}},
        }


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
