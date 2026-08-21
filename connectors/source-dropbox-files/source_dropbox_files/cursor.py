from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping
from copy import copy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.stream.cursor.abstract_file_based_cursor import (
    AbstractFileBasedCursor,
)

STATE_VERSION = 3


@dataclass(frozen=True)
class MigrationOperation:
    kind: str
    file_id: str
    old_path: str
    old_content_hash: str
    new_path: str | None = None
    new_content_hash: str | None = None

    def record(self) -> dict[str, str]:
        record = {
            "operation": self.kind,
            "file_id": self.file_id,
            "old_path": self.old_path,
            "old_content_hash": self.old_content_hash,
        }
        if self.new_path is not None:
            record["new_path"] = self.new_path
        if self.new_content_hash is not None:
            record["new_content_hash"] = self.new_content_hash
        return record


@dataclass(frozen=True)
class MigrationPlan:
    operations: list[MigrationOperation]
    files: list[RemoteFile]


class DropboxFileVersionCursor(AbstractFileBasedCursor):
    """Tracks the successfully staged Dropbox byte version for each stable file ID."""

    def __init__(self, stream_config: FileBasedStreamConfig, **_: Any) -> None:
        super().__init__(stream_config)
        self._files: dict[str, dict[str, str]] = {}
        self._scope: dict[str, Any] | None = None
        self._cursor: str | None = None
        self._legacy_scope = False

    def set_initial_state(self, value: MutableMapping[str, Any]) -> None:
        if not value:
            self._files = {}
            self._scope = None
            self._cursor = None
            self._legacy_scope = False
            return
        version = value.get("version")
        if version not in {1, 2, STATE_VERSION}:
            raise ValueError("Dropbox file-transfer state has an unsupported version.")
        raw_files = value.get("files")
        if not isinstance(raw_files, Mapping):
            raise ValueError("Dropbox file-transfer state requires a files object.")
        self._legacy_scope = version == 1
        self._scope = None if self._legacy_scope else _parse_scope(value.get("scope"))
        self._cursor = _parse_cursor(value.get("cursor")) if version == STATE_VERSION else None
        parsed: dict[str, dict[str, str]] = {}
        for file_id, entry in raw_files.items():
            if not isinstance(file_id, str) or not file_id:
                raise ValueError("Dropbox file-transfer state contains an invalid file_id.")
            if not isinstance(entry, Mapping):
                raise ValueError(f"Dropbox file-transfer state for {file_id!r} must be an object.")
            parsed[file_id] = {
                field: _required_state_string(entry, field, file_id)
                for field in ("path", "rev", "content_hash")
            }
            _validate_relative_path(parsed[file_id]["path"], file_id)
        self._files = parsed

    def add_file(self, file: RemoteFile) -> None:
        file_id, version = _version_for(file)
        self._files[file_id] = version

    def mark_move(self, operation: MigrationOperation) -> None:
        previous = self._files.get(operation.file_id)
        if previous is None or operation.kind != "move" or operation.new_path is None:
            raise ValueError("Dropbox file-transfer move operation does not match state.")
        self._files[operation.file_id] = {**previous, "path": operation.new_path}

    def mark_delete(self, operation: MigrationOperation) -> None:
        if operation.kind != "delete" or operation.file_id not in self._files:
            raise ValueError("Dropbox file-transfer delete operation does not match state.")
        del self._files[operation.file_id]

    def advance_cursor(self, cursor: str) -> None:
        if not isinstance(cursor, str) or not cursor:
            raise ValueError("Dropbox file-transfer cursor is invalid.")
        self._cursor = cursor

    @property
    def dropbox_cursor(self) -> str | None:
        return self._cursor

    @property
    def has_dropbox_cursor(self) -> bool:
        return self._cursor is not None and not self._legacy_scope

    def get_state(self) -> MutableMapping[str, Any]:
        state: MutableMapping[str, Any] = {
            "version": STATE_VERSION,
            "scope": self._scope or {"path": "", "recursive": True},
            "files": {file_id: self._files[file_id] for file_id in sorted(self._files)},
        }
        if self._cursor is not None:
            state["cursor"] = self._cursor
        return state

    def get_start_time(self) -> datetime:
        return datetime.min.replace(tzinfo=UTC)

    def get_files_to_sync(
        self, all_files: Iterable[RemoteFile], logger: Any
    ) -> Iterable[RemoteFile]:
        del logger
        for file in all_files:
            file_id, version = _version_for(file)
            previous = self._files.get(file_id)
            if previous is None:
                yield file
            elif _bytes_changed(previous, version):
                # Rename propagation is deferred. Keep writing changed bytes to the
                # original destination-relative path until a move policy exists.
                yield _with_transfer_path(file, previous["path"])

    def plan_inventory(
        self,
        all_files: Iterable[RemoteFile],
        *,
        rename_policy: str,
        delete_policy: str,
        path: str,
        recursive: bool,
    ) -> MigrationPlan:
        if rename_policy not in {"ignore", "propagate"}:
            raise ValueError("Dropbox file-transfer rename_policy is invalid.")
        if delete_policy not in {"ignore", "delete"}:
            raise ValueError("Dropbox file-transfer delete_policy is invalid.")
        scope = _current_scope(path, recursive)
        if self._legacy_scope:
            if rename_policy == "propagate" or delete_policy == "delete":
                raise ValueError(
                    "Dropbox file-transfer legacy state has no traversal scope; disable "
                    "rename/delete propagation for one fresh inventory."
                )
            self._files = {}
            self._scope = scope
            self._legacy_scope = False
        elif self._scope is None:
            self._scope = scope
        elif self._scope != scope:
            if rename_policy == "propagate" or delete_policy == "delete":
                raise ValueError(
                    "Dropbox file-transfer state scope does not match the configured "
                    "traversal scope."
                )
            self._scope = scope
            self._files = {}
        inventory: dict[str, tuple[RemoteFile, dict[str, str]]] = {}
        for file in all_files:
            file_id, version = _version_for(file)
            if file_id in inventory:
                raise ValueError(f"Dropbox inventory contains duplicate file_id {file_id!r}.")
            inventory[file_id] = (file, version)

        operations: list[MigrationOperation] = []
        files: list[RemoteFile] = []
        for file_id in sorted(inventory):
            file, current = inventory[file_id]
            previous = self._files.get(file_id)
            if previous is None:
                files.append(file)
                continue
            path_changed = previous["path"] != current["path"]
            bytes_changed = _bytes_changed(previous, current)
            if path_changed and rename_policy == "propagate":
                operations.append(
                    MigrationOperation(
                        "move",
                        file_id,
                        previous["path"],
                        previous["content_hash"],
                        current["path"],
                        current["content_hash"],
                    )
                )
                if bytes_changed:
                    files.append(file)
            elif bytes_changed:
                files.append(_with_transfer_path(file, previous["path"]))

        if delete_policy == "delete":
            for file_id in sorted(set(self._files) - set(inventory)):
                previous = self._files[file_id]
                operations.append(
                    MigrationOperation(
                        "delete", file_id, previous["path"], previous["content_hash"]
                    )
                )
        return MigrationPlan(operations=operations, files=files)

    def plan_delta(
        self,
        changed_files: Iterable[RemoteFile],
        deleted_paths: Iterable[str],
        *,
        rename_policy: str,
        delete_policy: str,
        path: str,
        recursive: bool,
    ) -> MigrationPlan:
        if rename_policy not in {"ignore", "propagate"}:
            raise ValueError("Dropbox file-transfer rename_policy is invalid.")
        if delete_policy not in {"ignore", "delete"}:
            raise ValueError("Dropbox file-transfer delete_policy is invalid.")
        scope = _current_scope(path, recursive)
        if self._scope != scope:
            raise ValueError(
                "Dropbox file-transfer state scope does not match the configured traversal scope."
            )

        operations: list[MigrationOperation] = []
        files: list[RemoteFile] = []
        consumed_rename_old_paths: set[str] = set()
        for file in changed_files:
            file_id, current = _version_for(file)
            previous = self._files.get(file_id)
            if previous is None:
                files.append(file)
                continue
            path_changed = previous["path"] != current["path"]
            bytes_changed = _bytes_changed(previous, current)
            if path_changed:
                consumed_rename_old_paths.add(previous["path"].casefold())
                if rename_policy == "propagate":
                    operations.append(
                        MigrationOperation(
                            "move",
                            file_id,
                            previous["path"],
                            previous["content_hash"],
                            current["path"],
                            current["content_hash"],
                        )
                    )
                    if bytes_changed:
                        files.append(file)
                elif bytes_changed:
                    files.append(_with_transfer_path(file, previous["path"]))
            elif bytes_changed:
                files.append(_with_transfer_path(file, previous["path"]))

        if delete_policy == "delete":
            by_path = {
                entry["path"].casefold(): file_id for file_id, entry in self._files.items()
            }
            for deleted_path in sorted(set(deleted_paths), key=str.casefold):
                normalized_deleted_path = deleted_path.casefold()
                if normalized_deleted_path in consumed_rename_old_paths:
                    continue
                file_id = by_path.get(normalized_deleted_path)
                if file_id is None:
                    continue
                previous = self._files[file_id]
                operations.append(
                    MigrationOperation(
                        "delete", file_id, previous["path"], previous["content_hash"]
                    )
                )
        return MigrationPlan(operations=operations, files=files)


def _version_for(file: RemoteFile) -> tuple[str, dict[str, str]]:
    file_id = getattr(file, "id", None)
    rev = getattr(file, "rev", None)
    content_hash = getattr(file, "content_hash", None)
    if not all(isinstance(value, str) and value for value in (file_id, rev, content_hash)):
        raise ValueError("Dropbox file metadata requires non-empty id, rev, and content_hash.")
    path = file.uri
    _validate_relative_path(path, file_id)
    return file_id, {"path": path, "rev": rev, "content_hash": content_hash}


def _bytes_changed(previous: Mapping[str, str], current: Mapping[str, str]) -> bool:
    return previous["rev"] != current["rev"] or previous["content_hash"] != current["content_hash"]


def _with_transfer_path(file: RemoteFile, path: str) -> RemoteFile:
    """Copy the file so current Dropbox metadata is never mutated for a pinned target path."""
    if hasattr(file, "copy"):
        return file.copy(update={"uri": path})
    transferred = copy(file)
    transferred.uri = path
    return transferred


def _required_state_string(entry: Mapping[str, Any], field: str, file_id: str) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Dropbox file-transfer state for {file_id!r} requires non-empty {field}.")
    return value


def _parse_scope(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("Dropbox file-transfer state requires a scope object.")
    path = value.get("path")
    recursive = value.get("recursive")
    if not isinstance(path, str) or not isinstance(recursive, bool):
        raise ValueError("Dropbox file-transfer state scope is invalid.")
    return _current_scope(path, recursive)


def _parse_cursor(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("Dropbox file-transfer state requires a non-empty cursor.")
    return value


def _current_scope(path: str, recursive: bool) -> dict[str, Any]:
    return {"path": path.rstrip("/"), "recursive": recursive}


def _validate_relative_path(path: str, file_id: str) -> None:
    if (
        path.startswith("/")
        or "\\" in path
        or "//" in path
        or any(segment in {"", ".", ".."} for segment in path.split("/"))
    ):
        raise ValueError(f"Dropbox file-transfer state for {file_id!r} contains an invalid path.")
