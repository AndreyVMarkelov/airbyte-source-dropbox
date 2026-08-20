from types import SimpleNamespace

import pytest
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.config.unstructured_format import UnstructuredFormat

from source_dropbox_files.cursor import DropboxFileVersionCursor, MigrationOperation
from source_dropbox_files.source import DropboxIncrementalFileTransferStream


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
        "version": 2,
        "scope": {"path": "", "recursive": True},
        "files": {
            "id:a": {"path": "new.bin", "rev": "rev-1", "content_hash": "hash-1"},
            "id:z": {"path": "nested/report.pdf", "rev": "rev-1", "content_hash": "hash-1"},
        },
    }


def test_unchanged_and_path_only_changed_files_are_skipped() -> None:
    cursor = _cursor()
    cursor.set_initial_state(
        {
            "version": 2,
            "scope": {"path": "/Exports", "recursive": True},
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
            "version": 2,
            "scope": {"path": "/Exports", "recursive": True},
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
            "version": 2,
            "scope": {"path": "/Exports", "recursive": True},
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
        {"version": 1, "files": {}},
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


def test_plan_ignores_rename_and_deletion_by_default() -> None:
    cursor = _cursor()
    cursor.set_initial_state(
        {
            "version": 2,
            "scope": {"path": "/Exports", "recursive": True},
            "files": {
                "id:renamed": {"path": "old.pdf", "rev": "r1", "content_hash": "h1"},
                "id:deleted": {"path": "gone.pdf", "rev": "r1", "content_hash": "h2"},
            },
        }
    )

    plan = cursor.plan_inventory(
        [_file(file_id="id:renamed", path="new.pdf", rev="r1", content_hash="h1")],
        rename_policy="ignore",
        delete_policy="ignore",
        path="/Exports",
        recursive=True,
    )

    assert plan.operations == []
    assert plan.files == []
    assert cursor.get_state()["files"]["id:renamed"]["path"] == "old.pdf"


def test_plan_propagates_rename_then_transfers_changed_bytes_and_updates_state() -> None:
    cursor = _cursor()
    cursor.set_initial_state(
        {
            "version": 2,
            "scope": {"path": "/Exports", "recursive": True},
            "files": {
                "id:file": {"path": "old.pdf", "rev": "r1", "content_hash": "h1"}
            },
        }
    )
    current = _file(path="new.pdf", rev="r2", content_hash="h2")

    plan = cursor.plan_inventory(
        [current],
        rename_policy="propagate",
        delete_policy="ignore",
        path="/Exports",
        recursive=True,
    )

    assert [operation.record() for operation in plan.operations] == [
        {
            "operation": "move",
            "file_id": "id:file",
            "old_path": "old.pdf",
            "old_content_hash": "h1",
            "new_path": "new.pdf",
            "new_content_hash": "h2",
        }
    ]
    assert plan.files == [current]
    cursor.mark_move(plan.operations[0])
    cursor.add_file(current)
    assert cursor.get_state()["files"]["id:file"] == {
        "path": "new.pdf",
        "rev": "r2",
        "content_hash": "h2",
    }


def test_plan_deletes_only_absent_ids_after_complete_inventory() -> None:
    cursor = _cursor()
    cursor.set_initial_state(
        {
            "version": 2,
            "scope": {"path": "/Exports", "recursive": True},
            "files": {
                "id:present": {"path": "present.pdf", "rev": "r1", "content_hash": "h1"},
                "id:gone": {"path": "gone.pdf", "rev": "r1", "content_hash": "h2"},
            },
        }
    )

    plan = cursor.plan_inventory(
        [_file(file_id="id:present", path="present.pdf", rev="r1", content_hash="h1")],
        rename_policy="ignore",
        delete_policy="delete",
        path="/Exports",
        recursive=True,
    )

    assert plan.operations[0].record() == {
        "operation": "delete",
        "file_id": "id:gone",
        "old_path": "gone.pdf",
        "old_content_hash": "h2",
    }
    cursor.mark_delete(plan.operations[0])
    assert "id:gone" not in cursor.get_state()["files"]


def test_move_control_record_precedes_its_source_state_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _cursor()
    cursor.set_initial_state(
        {
            "version": 2,
            "scope": {"path": "/Exports", "recursive": True},
            "files": {
                "id:file": {"path": "old.pdf", "rev": "r1", "content_hash": "h1"}
            },
        }
    )
    stream = DropboxIncrementalFileTransferStream.__new__(DropboxIncrementalFileTransferStream)
    stream.config = FileBasedStreamConfig(name="raw_files", format=UnstructuredFormat())
    stream._cursor = cursor
    operation = MigrationOperation("move", "id:file", "old.pdf", "h1", "new.pdf", "h1")
    monkeypatch.setattr(
        "source_dropbox_files.source.DefaultFileBasedStream.read_records_from_slice",
        lambda *_args: iter(()),
    )

    records = stream.read_records_from_slice({"operations": [operation], "files": []})
    message = next(records)
    assert message.record.data["operation"] == "move"
    assert cursor.get_state()["files"]["id:file"]["path"] == "old.pdf"
    assert list(records) == []
    assert cursor.get_state()["files"]["id:file"]["path"] == "new.pdf"


def test_scope_mismatch_fails_before_delete_controls_are_planned() -> None:
    cursor = _cursor()
    cursor.set_initial_state(
        {
            "version": 2,
            "scope": {"path": "/Company", "recursive": True},
            "files": {
                "id:file": {"path": "report.pdf", "rev": "r1", "content_hash": "h1"}
            },
        }
    )

    with pytest.raises(ValueError, match="scope does not match"):
        cursor.plan_inventory(
            [],
            rename_policy="ignore",
            delete_policy="delete",
            path="/Other",
            recursive=True,
        )
    assert "id:file" in cursor.get_state()["files"]


def test_scope_mismatch_resets_non_propagating_incremental_state() -> None:
    cursor = _cursor()
    cursor.set_initial_state(
        {
            "version": 2,
            "scope": {"path": "/Company", "recursive": True},
            "files": {
                "id:file": {"path": "report.pdf", "rev": "r1", "content_hash": "h1"}
            },
        }
    )
    current = _file(file_id="id:new", path="new.pdf")

    plan = cursor.plan_inventory(
        [current],
        rename_policy="ignore",
        delete_policy="ignore",
        path="/Other",
        recursive=False,
    )

    assert plan.operations == []
    assert plan.files == [current]
    assert cursor.get_state()["scope"] == {"path": "/Other", "recursive": False}
