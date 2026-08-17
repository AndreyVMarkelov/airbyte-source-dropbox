from __future__ import annotations

from datetime import datetime
from typing import Any

from dropbox.files import DeletedMetadata, FileMetadata, FolderMetadata, Metadata


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _metadata_base(entry: Metadata) -> dict[str, Any]:
    return {
        "name": entry.name,
        "path_lower": entry.path_lower,
        "path_display": entry.path_display,
    }


def _normalize_file_sharing_info(entry: FileMetadata) -> dict[str, Any] | None:
    if not entry.sharing_info:
        return None
    return {
        "read_only": entry.sharing_info.read_only,
        "parent_shared_folder_id": entry.sharing_info.parent_shared_folder_id,
        "modified_by": entry.sharing_info.modified_by,
    }


def _normalize_folder_sharing_info(entry: FolderMetadata) -> dict[str, Any] | None:
    if not entry.sharing_info:
        return None
    return {
        "read_only": entry.sharing_info.read_only,
        "parent_shared_folder_id": entry.sharing_info.parent_shared_folder_id,
        "shared_folder_id": entry.sharing_info.shared_folder_id,
        "traverse_only": entry.sharing_info.traverse_only,
        "no_access": entry.sharing_info.no_access,
    }


def normalize_file(entry: FileMetadata) -> dict[str, Any]:
    """Normalize Dropbox file metadata for the current-state snapshot stream."""
    file_lock_info = entry.file_lock_info
    return {
        **_metadata_base(entry),
        "id": entry.id,
        "rev": entry.rev,
        "client_modified": _isoformat(entry.client_modified),
        "server_modified": _isoformat(entry.server_modified),
        "size": entry.size,
        "content_hash": entry.content_hash,
        "is_downloadable": entry.is_downloadable,
        "has_explicit_shared_members": entry.has_explicit_shared_members,
        "sharing_info": _normalize_file_sharing_info(entry),
        "file_lock_info": (
            {
                "is_lockholder": file_lock_info.is_lockholder,
                "lockholder_name": file_lock_info.lockholder_name,
                "lockholder_account_id": file_lock_info.lockholder_account_id,
                "created": _isoformat(file_lock_info.created),
            }
            if file_lock_info
            else None
        ),
    }


def normalize_folder(entry: FolderMetadata) -> dict[str, Any]:
    """Normalize Dropbox folder metadata for the current-state snapshot stream."""
    return {
        **_metadata_base(entry),
        "id": entry.id,
        "shared_folder_id": entry.shared_folder_id,
        "sharing_info": _normalize_folder_sharing_info(entry),
    }


def normalize_entry(entry: Metadata) -> dict[str, Any]:
    """Normalize a Dropbox listing entry without changing the public entries contract."""
    if isinstance(entry, FileMetadata):
        file = normalize_file(entry)
        return {
            "entry_key": f"file:{file['id']}",
            "entry_type": "file",
            "operation": "upsert",
            **{
                field: file[field]
                for field in (
                    "id",
                    "name",
                    "path_lower",
                    "path_display",
                    "rev",
                    "client_modified",
                    "server_modified",
                    "size",
                    "content_hash",
                    "is_downloadable",
                )
            },
        }

    if isinstance(entry, FolderMetadata):
        folder = normalize_folder(entry)
        return {
            "entry_key": f"folder:{folder['id']}",
            "entry_type": "folder",
            "operation": "upsert",
            **{
                field: folder[field]
                for field in ("id", "name", "path_lower", "path_display", "shared_folder_id")
            },
        }

    if isinstance(entry, DeletedMetadata):
        return {
            **_metadata_base(entry),
            "entry_key": f"deleted:{entry.path_lower}",
            "entry_type": "deleted",
            "operation": "delete",
            "id": None,
        }

    raise TypeError(f"Unsupported Dropbox metadata type: {type(entry).__name__}")
