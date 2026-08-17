from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import dropbox
from dropbox.files import ListFolderResult, Metadata


@dataclass(frozen=True)
class DropboxPage:
    entries: list[Metadata]
    cursor: str
    has_more: bool


class DropboxClient:
    def __init__(self, config: dict[str, Any]) -> None:
        credentials = config["credentials"]
        auth_type = credentials["auth_type"]

        common_kwargs = {
            "max_retries_on_error": 5,
            "max_retries_on_rate_limit": 5,
        }

        if auth_type == "oauth2_pkce":
            self._client = dropbox.Dropbox(
                oauth2_refresh_token=credentials["refresh_token"],
                app_key=credentials["app_key"],
                **common_kwargs,
            )
        elif auth_type == "access_token":
            self._client = dropbox.Dropbox(
                oauth2_access_token=credentials["access_token"],
                **common_kwargs,
            )
        else:
            raise ValueError(f"Unsupported auth_type: {auth_type}")

    def current_account(self) -> Any:
        return self._client.users_get_current_account()

    def list_folder(self, path: str, recursive: bool, include_deleted: bool) -> DropboxPage:
        result = self._client.files_list_folder(
            path=path,
            recursive=recursive,
            include_deleted=include_deleted,
        )
        return self._to_page(result)

    def list_folder_continue(self, cursor: str) -> DropboxPage:
        result = self._client.files_list_folder_continue(cursor)
        return self._to_page(result)

    def iter_entries(
        self,
        *,
        path: str,
        recursive: bool,
        include_deleted: bool,
        cursor: str | None = None,
    ) -> Iterator[DropboxPage]:
        page = (
            self.list_folder_continue(cursor)
            if cursor
            else self.list_folder(path, recursive, include_deleted)
        )

        while True:
            yield page
            if not page.has_more:
                break
            page = self.list_folder_continue(page.cursor)

    @staticmethod
    def _to_page(result: ListFolderResult) -> DropboxPage:
        return DropboxPage(
            entries=list(result.entries),
            cursor=result.cursor,
            has_more=result.has_more,
        )
