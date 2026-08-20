from types import SimpleNamespace

import pytest
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.config.unstructured_format import UnstructuredFormat

from source_dropbox_files.cursor import DropboxFileVersionCursor


def _cursor() -> DropboxFileVersionCursor:
    return DropboxFileVersionCursor(
        FileBasedStreamConfig(name="raw_files", format=UnstructuredFormat())
    )


def _file(
    file_id: str = "id:file",
    path: str = "nested/report.pdf",
    rev: str = "rev-1",
    content_hash: str = "hash-1",
) -> SimpleNamespace:
    return SimpleNamespace(id=file_id, uri=path, rev=rev, content_hash=content_hash)


def test_initial_run_transfers_all_files_and_serializes_deterministic_state() -> None:
    cursor = _cursor()
    first = _file(file_id="id:z")
    second = _file(file_id="id:a", path="new.bin")

    assert list(cursor.get_files_to_sync([first, second], None)) == [first, second]
    cursor.add_file(first)
    cursor.add_file(second)

    assert cursor.get_state() == {
        "version": 1,
        "files": {
            "id:a": {"path": "new.bin", "rev": "rev-1", "content_hash": "hash-1"},
            "id:z": {"path": "nested/report.pdf", "rev": "rev-1", "content_hash": "hash-1"},
        },
    }


def test_unchanged_and_path_only_changed_files_are_skipped() -> None:
    cursor = _cursor()
    cursor.set_initial_state(
        {
            "version": 1,
            "files": {
                "id:file": {"path": "old.pdf", "rev": "rev-1", "content_hash": "hash-1"}
            },
        }
    )

    assert list(cursor.get_files_to_sync([_file(path="old.pdf")], None)) == []
    assert list(cursor.get_files_to_sync([_file(path="renamed.pdf")], None)) == []
    assert cursor.get_state()["files"]["id:file"]["path"] == "old.pdf"


def test_changed_renamed_file_uses_pinned_target_path_and_updates_only_byte_version() -> None:
    cursor = _cursor()
    cursor.set_initial_state(
        {
            "version": 1,
            "files": {
                "id:file": {
                    "path": "old/report.pdf",
                    "rev": "rev-1",
                    "content_hash": "hash-1",
                }
            },
        }
    )
    current = _file(path="new/report.pdf", rev="rev-2", content_hash="hash-2")

    transferred = list(cursor.get_files_to_sync([current], None))

    assert transferred[0] is not current
    assert current.uri == "new/report.pdf"
    assert transferred[0].uri == "old/report.pdf"
    cursor.add_file(transferred[0])
    assert cursor.get_state()["files"]["id:file"] == {
        "path": "old/report.pdf",
        "rev": "rev-2",
        "content_hash": "hash-2",
    }


@pytest.mark.parametrize("field, value", [("rev", "rev-2"), ("content_hash", "hash-2")])
def test_changed_byte_version_and_new_file_are_transferred(field: str, value: str) -> None:
    cursor = _cursor()
    cursor.set_initial_state(
        {
            "version": 1,
            "files": {
                "id:file": {"path": "report.pdf", "rev": "rev-1", "content_hash": "hash-1"}
            },
        }
    )
    changed = _file(**{field: value})
    new = _file(file_id="id:new", path="new.pdf")

    transferred = list(cursor.get_files_to_sync([changed, new], None))
    assert transferred[0].uri == "report.pdf"
    assert transferred[1] is new


def test_empty_legacy_state_starts_a_first_sync() -> None:
    cursor = _cursor()
    cursor.set_initial_state({})
    assert list(cursor.get_files_to_sync([_file()], None)) == [_file()]


@pytest.mark.parametrize(
    "state",
    [
        {"version": 2, "files": {}},
        {"version": 1, "files": []},
        {"version": 1, "files": {"": {"path": "a", "rev": "r", "content_hash": "h"}}},
        {"version": 1, "files": {"id": {"path": "../a", "rev": "r", "content_hash": "h"}}},
        {"version": 1, "files": {"id": {"path": "a"}}},
    ],
)
def test_invalid_state_is_rejected(state: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="Dropbox file-transfer state"):
        _cursor().set_initial_state(state)
