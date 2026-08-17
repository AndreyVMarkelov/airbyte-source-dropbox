from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import dropbox
from dropbox.exceptions import ApiError, AuthError, RateLimitError
from dropbox.files import ListFolderContinueError, ListFolderResult, Metadata
from dropbox.sharing import (
    ListFoldersResult,
    ListSharedLinksResult,
    SharedFolderMetadata,
    SharedLinkMetadata,
)


@dataclass(frozen=True)
class DropboxPage:
    entries: list[Metadata]
    cursor: str
    has_more: bool


@dataclass(frozen=True)
class SharedLinksPage:
    links: list[SharedLinkMetadata]
    cursor: str | None
    has_more: bool


@dataclass(frozen=True)
class SharedFoldersPage:
    entries: list[SharedFolderMetadata]
    cursor: str | None


class DropboxAuthenticationError(RuntimeError):
    """Raised when the Dropbox credentials are invalid or revoked."""


class DropboxRateLimitError(RuntimeError):
    """Raised only after the Dropbox SDK exhausts its rate-limit retries."""


class DropboxCursorResetError(RuntimeError):
    """Raised when Dropbox invalidates a list-folder cursor."""


class DropboxSharingPermissionError(RuntimeError):
    """Raised when the Dropbox app cannot read sharing metadata."""


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
        try:
            return self._client.users_get_current_account()
        except AuthError as exc:
            raise DropboxAuthenticationError("Dropbox rejected the supplied credentials.") from exc
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited the connection check.") from exc

    def list_folder(self, path: str, recursive: bool, include_deleted: bool) -> DropboxPage:
        try:
            result = self._client.files_list_folder(
                path=path,
                recursive=recursive,
                include_deleted=include_deleted,
            )
        except AuthError as exc:
            raise DropboxAuthenticationError("Dropbox rejected the supplied credentials.") from exc
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited folder synchronization.") from exc
        return self._to_page(result)

    def list_folder_continue(self, cursor: str) -> DropboxPage:
        try:
            result = self._client.files_list_folder_continue(cursor)
        except AuthError as exc:
            raise DropboxAuthenticationError("Dropbox rejected the supplied credentials.") from exc
        except ApiError as exc:
            if isinstance(exc.error, ListFolderContinueError) and exc.error.is_reset():
                raise DropboxCursorResetError(
                    "Dropbox invalidated the saved folder cursor."
                ) from exc
            raise
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited folder synchronization.") from exc
        return self._to_page(result)

    def iter_entries(
        self,
        *,
        path: str,
        recursive: bool,
        include_deleted: bool,
        cursor: str | None = None,
    ) -> Iterator[DropboxPage]:
        reset_recovered = False
        try:
            page = (
                self.list_folder_continue(cursor)
                if cursor
                else self.list_folder(path, recursive, include_deleted)
            )
        except DropboxCursorResetError:
            page = self.list_folder(path, recursive, include_deleted)
            reset_recovered = True

        while True:
            yield page
            if not page.has_more:
                break
            try:
                page = self.list_folder_continue(page.cursor)
            except DropboxCursorResetError:
                if reset_recovered:
                    raise
                # A reset cannot safely resume a partial listing. Restarting from the
                # configured root may replay records, but cannot skip records.
                page = self.list_folder(path, recursive, include_deleted)
                reset_recovered = True

    def iter_shared_links(self) -> Iterator[SharedLinksPage]:
        """List the authenticated account's shared-link inventory."""
        cursor: str | None = None
        while True:
            try:
                result = self._client.sharing_list_shared_links(cursor=cursor)
            except AuthError as exc:
                raise DropboxSharingPermissionError(
                    "Dropbox app requires sharing.read to sync sharing streams."
                ) from exc
            except RateLimitError as exc:
                raise DropboxRateLimitError(
                    "Dropbox rate limited sharing synchronization."
                ) from exc
            page = self._to_shared_links_page(result)
            yield page
            if not page.has_more:
                break
            cursor = page.cursor
            if cursor is None:
                raise RuntimeError("Dropbox returned shared-link pagination without a cursor.")

    def iter_shared_folders(self) -> Iterator[SharedFoldersPage]:
        """List all shared folders available to the authenticated account."""
        try:
            result = self._client.sharing_list_folders()
        except AuthError as exc:
            raise DropboxSharingPermissionError(
                "Dropbox app requires sharing.read to sync sharing streams."
            ) from exc
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited sharing synchronization.") from exc
        while True:
            page = self._to_shared_folders_page(result)
            yield page
            if page.cursor is None:
                break
            try:
                result = self._client.sharing_list_folders_continue(page.cursor)
            except AuthError as exc:
                raise DropboxSharingPermissionError(
                    "Dropbox app requires sharing.read to sync sharing streams."
                ) from exc
            except RateLimitError as exc:
                raise DropboxRateLimitError(
                    "Dropbox rate limited sharing synchronization."
                ) from exc

    @staticmethod
    def _to_page(result: ListFolderResult) -> DropboxPage:
        return DropboxPage(
            entries=list(result.entries),
            cursor=result.cursor,
            has_more=result.has_more,
        )

    @staticmethod
    def _to_shared_links_page(result: ListSharedLinksResult) -> SharedLinksPage:
        return SharedLinksPage(
            links=list(result.links), cursor=result.cursor, has_more=result.has_more
        )

    @staticmethod
    def _to_shared_folders_page(result: ListFoldersResult) -> SharedFoldersPage:
        return SharedFoldersPage(entries=list(result.entries), cursor=result.cursor)
