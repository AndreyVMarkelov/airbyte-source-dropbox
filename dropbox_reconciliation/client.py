from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

import dropbox
from dropbox.exceptions import ApiError, AuthError, BadInputError, RateLimitError
from dropbox.files import FileMetadata, FolderMetadata

from dropbox_reconciliation.models import FileInventoryItem, Inventory, InventoryPathIssue


class ReconciliationError(RuntimeError):
    """An operational reconciliation failure that prevents a complete report."""


class DuplicateNormalizedPathError(ReconciliationError):
    """Two files on one side cannot be compared unambiguously."""


class DropboxReconciliationClient:
    def __init__(self, config: Mapping[str, Any], side: Literal["source", "destination"]) -> None:
        self.side = side
        credentials = config.get("credentials")
        if not isinstance(credentials, Mapping):
            raise ReconciliationError(f"{side} credentials are required.")
        auth_type = credentials.get("auth_type")
        if auth_type == "oauth2_pkce":
            app_key = credentials.get("app_key")
            refresh_token = credentials.get("refresh_token")
            if (
                not isinstance(app_key, str)
                or not app_key
                or not isinstance(refresh_token, str)
                or not refresh_token
            ):
                raise ReconciliationError(f"{side} OAuth app_key and refresh_token are required.")
            self._client = dropbox.Dropbox(
                oauth2_refresh_token=refresh_token,
                app_key=app_key,
                max_retries_on_error=0,
                max_retries_on_rate_limit=0,
            )
        elif auth_type == "access_token":
            access_token = credentials.get("access_token")
            if not isinstance(access_token, str) or not access_token:
                raise ReconciliationError(f"{side} access_token is required.")
            self._client = dropbox.Dropbox(
                oauth2_access_token=access_token,
                max_retries_on_error=0,
                max_retries_on_rate_limit=0,
            )
        else:
            raise ReconciliationError(f"{side} credentials use an unsupported auth_type.")

    def validate_root(self, root_path: object) -> str:
        root = _normalize_root(root_path)
        try:
            self._client.users_get_current_account()
            if root:
                metadata = self._client.files_get_metadata(root)
        except (AuthError, BadInputError) as exc:
            raise ReconciliationError(
                f"{self.side} Dropbox credentials are invalid or revoked."
            ) from exc
        except RateLimitError as exc:
            raise ReconciliationError(f"{self.side} Dropbox rate limited root validation.") from exc
        except ApiError as exc:
            raise ReconciliationError(
                f"{self.side} root_path does not exist or is inaccessible."
            ) from exc
        if root and not isinstance(metadata, FolderMetadata):
            raise ReconciliationError(f"{self.side} root_path must refer to a Dropbox folder.")
        return root

    def inventory(self, root: str) -> Inventory:
        try:
            page = self._client.files_list_folder(root, recursive=True, include_deleted=False)
            entries = list(page.entries)
            while page.has_more:
                page = self._client.files_list_folder_continue(page.cursor)
                entries.extend(page.entries)
        except (AuthError, BadInputError) as exc:
            raise ReconciliationError(
                f"{self.side} Dropbox credentials are invalid or revoked."
            ) from exc
        except RateLimitError as exc:
            raise ReconciliationError(f"{self.side} Dropbox rate limited inventory.") from exc
        except ApiError as exc:
            raise ReconciliationError(f"{self.side} Dropbox inventory failed.") from exc

        files: dict[str, FileInventoryItem] = {}
        issues: list[InventoryPathIssue] = []
        for index, entry in enumerate(entries):
            if not isinstance(entry, FileMetadata):
                continue
            item = _file_item(entry, root)
            if item is None:
                fallback = entry.path_display or entry.path_lower or entry.id
                issues.append(
                    InventoryPathIssue(
                        sort_key=f"~invalid-{self.side}-{index:08d}",
                        path=fallback,
                        side=self.side,
                        item=_unsafe_item(entry, fallback),
                    )
                )
                continue
            if item.normalized_path in files:
                raise DuplicateNormalizedPathError(
                    f"duplicate_normalized_path in {self.side} inventory: {item.normalized_path}"
                )
            files[item.normalized_path] = item
        return Inventory(files=files, issues=issues)


def _normalize_root(value: object) -> str:
    if not isinstance(value, str):
        raise ReconciliationError("root_path must be a string.")
    if value == "":
        return ""
    if not value.startswith("/") or "\\" in value or "//" in value:
        raise ReconciliationError(
            "root_path must be an absolute POSIX path without repeated separators."
        )
    segments = value.split("/")[1:]
    if any(segment in {"", ".", ".."} for segment in segments):
        raise ReconciliationError("root_path contains an invalid path segment.")
    return value.rstrip("/")


def _file_item(entry: FileMetadata, root: str) -> FileInventoryItem | None:
    path_lower = entry.path_lower
    path_display = entry.path_display
    if not path_lower or not path_display:
        return None
    prefix = root.lower()
    if root:
        if not path_lower.startswith(f"{prefix}/"):
            return None
        normalized_path = path_lower[len(prefix) + 1 :].lower()
        display_path = (
            path_display[len(root) + 1 :]
            if path_display.startswith(f"{root}/")
            else normalized_path
        )
    else:
        if not path_lower.startswith("/"):
            return None
        normalized_path = path_lower[1:].lower()
        display_path = path_display[1:] if path_display.startswith("/") else normalized_path
    if not normalized_path or _has_invalid_segment(normalized_path):
        return None
    return FileInventoryItem(
        normalized_path=normalized_path,
        display_path=display_path,
        file_id=entry.id,
        rev=entry.rev,
        size=entry.size,
        content_hash=entry.content_hash,
        client_modified=entry.client_modified,
        server_modified=entry.server_modified,
    )


def _unsafe_item(entry: FileMetadata, path: str) -> FileInventoryItem:
    return FileInventoryItem(
        normalized_path=path.lower(),
        display_path=path,
        file_id=entry.id,
        rev=entry.rev,
        size=entry.size,
        content_hash=entry.content_hash,
        client_modified=entry.client_modified,
        server_modified=entry.server_modified,
    )


def _has_invalid_segment(path: str) -> bool:
    return "\\" in path or "//" in path or any(part in {"", ".", ".."} for part in path.split("/"))
