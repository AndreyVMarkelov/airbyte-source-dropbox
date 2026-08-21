from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dropbox.files import FileMetadata, FolderMetadata

from source_dropbox_files.reader import SourceDropboxFilesStreamReader
from source_dropbox_files.source import DropboxIncrementalFileTransferStream
from source_dropbox_files.spec import SourceDropboxFilesSpec


def _config(**overrides: object) -> SourceDropboxFilesSpec:
    config: dict[str, object] = {
        "credentials": {"auth_type": "access_token", "access_token": "token"},
        "path": "/Exports",
        "recursive": True,
    }
    config.update(overrides)
    return SourceDropboxFilesSpec(**config)


def _file(name: str = "report.pdf", size: int = 4) -> FileMetadata:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    return FileMetadata(
        name=name,
        id="id:file",
        path_lower=f"/exports/{name}",
        path_display=f"/Exports/{name}",
        rev="0123456789",
        size=size,
        client_modified=timestamp,
        server_modified=timestamp,
        content_hash="0" * 64,
    )


def _reader() -> SourceDropboxFilesStreamReader:
    reader = SourceDropboxFilesStreamReader(sleeper=Mock())
    reader.config = _config()
    reader._client = Mock()
    return reader


def test_lists_live_files_relative_to_root_and_excludes_folders() -> None:
    reader = _reader()
    reader._client.files_list_folder.return_value = SimpleNamespace(
        entries=[
            _file(),
            FolderMetadata(
                name="folder",
                id="id:folder",
                path_lower="/exports/folder",
                path_display="/Exports/folder",
            ),
        ],
        has_more=False,
    )

    files = list(reader.get_matching_files(["**"], None, Mock()))

    assert len(files) == 1
    assert files[0].uri == "report.pdf"
    assert files[0].id == "id:file"
    assert files[0].client_modified == "2026-01-01T00:00:00Z"
    assert files[0].server_modified == "2026-01-01T00:00:00Z"
    reader._client.files_list_folder.assert_called_once_with(
        "/Exports", recursive=True, include_deleted=False
    )


def test_malformed_server_modified_is_omitted_without_blocking_file_transfer() -> None:
    entry = SimpleNamespace(
        name="report.pdf",
        id="id:file",
        path_lower="/exports/report.pdf",
        path_display="/Exports/report.pdf",
        rev="0123456789",
        size=4,
        content_hash="0" * 64,
        client_modified=datetime(2026, 1, 1, tzinfo=UTC),
        server_modified=None,
    )

    remote = _reader()._remote_file(entry)

    assert remote.client_modified == "2026-01-01T00:00:00Z"
    assert remote.server_modified is None


def test_raw_file_stream_advertises_incremental_sync() -> None:
    stream = DropboxIncrementalFileTransferStream.__new__(DropboxIncrementalFileTransferStream)
    assert stream.supports_incremental is True


def test_oversized_files_are_skipped_before_download() -> None:
    reader = _reader()
    reader.config = _config(file_transfer={"max_file_size_mb": 1})
    reader._client = Mock()
    reader._client.files_list_folder.return_value = SimpleNamespace(
        entries=[_file(size=1024 * 1024 + 1)], has_more=False
    )

    assert list(reader.get_matching_files(["**"], None, Mock())) == []


def test_download_streams_chunks_and_calculates_sha256(tmp_path: Path) -> None:
    reader = _reader()
    reader._client.files_download.return_value = (
        _file(),
        SimpleNamespace(iter_content=lambda chunk_size: [b"ab", b"cd"]),
    )
    remote = reader._remote_file(_file())
    path = tmp_path / "staged.bin"

    remote.download_to_local_directory(str(path))

    assert path.read_bytes() == b"abcd"
    assert remote.downloaded_sha256 == (
        "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589"
    )
    reader._client.files_download.assert_called_once_with("id:file")


@pytest.mark.parametrize("chunk_size", [0, 33])
def test_chunk_size_configuration_is_bounded(chunk_size: int) -> None:
    with pytest.raises(ValueError, match="download_chunk_size_mb"):
        _config(file_transfer={"download_chunk_size_mb": chunk_size})
