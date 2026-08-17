from __future__ import annotations

from typing import Any

from dropbox.files import DeletedMetadata, FileMetadata, FolderMetadata, Metadata


def normalize_entry(entry: Metadata) -> dict[str, Any]:
    base: dict[str, Any] = {
        "name": entry.name,
        "path_lower": entry.path_lower,
        "path_display": entry.path_display,
    }

    if isinstance(entry, FileMetadata):
        return {
            **base,
            "entry_key": f"file:{entry.id}",
            "entry_type": "file",
            "operation": "upsert",
            "id": entry.id,
            "rev": entry.rev,
            "client_modified": entry.client_modified.isoformat(),
            "server_modified": entry.server_modified.isoformat(),
            "size": entry.size,
            "content_hash": entry.content_hash,
            "is_downloadable": entry.is_downloadable,
        }

    if isinstance(entry, FolderMetadata):
        return {
            **base,
            "entry_key": f"folder:{entry.id}",
            "entry_type": "folder",
            "operation": "upsert",
            "id": entry.id,
            "shared_folder_id": entry.shared_folder_id,
        }

    if isinstance(entry, DeletedMetadata):
        return {
            **base,
            "entry_key": f"deleted:{entry.path_lower}",
            "entry_type": "deleted",
            "operation": "delete",
            "id": None,
        }

    raise TypeError(f"Unsupported Dropbox metadata type: {type(entry).__name__}")
