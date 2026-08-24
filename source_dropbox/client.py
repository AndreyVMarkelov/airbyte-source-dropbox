from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from time import monotonic, sleep
from typing import Any

from dropbox.exceptions import ApiError, AuthError, BadInputError, RateLimitError
from dropbox.file_properties import GetTemplateResult, TemplateFilter
from dropbox.files import ListFolderContinueError, ListFolderResult, Metadata
from dropbox.riviera import FileIdOrUrl, GetMarkdownAsyncCheckResult
from dropbox.sharing import (
    ListFolderMembersContinueError,
    ListFoldersContinueError,
    ListFoldersResult,
    ListSharedLinksError,
    ListSharedLinksResult,
    SharedFolderMembers,
    SharedFolderMetadata,
    SharedLinkMetadata,
)

from source_dropbox.dropbox_context import build_dropbox_client, effective_context_key


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


@dataclass(frozen=True)
class SharedFolderMembersPage:
    users: list[Any]
    groups: list[Any]
    invitees: list[Any]
    cursor: str | None


@dataclass(frozen=True)
class MarkdownExtraction:
    markdown: str | None
    extraction_status: str
    error_type: str | None = None
    error_code: str | None = None
    error_details: dict[str, Any] | None = None
    error_message: str | None = None


class DropboxAuthenticationError(RuntimeError):
    """Raised when the base Dropbox credentials are invalid, revoked, or insufficient."""


class DropboxRateLimitError(RuntimeError):
    """Raised only after the Dropbox SDK exhausts its rate-limit retries."""


class DropboxCursorResetError(RuntimeError):
    """Raised when Dropbox invalidates a list-folder cursor."""


class DropboxSharingPermissionError(RuntimeError):
    """Raised when the Dropbox app cannot read sharing metadata."""


class DropboxSharingAclError(RuntimeError):
    """Raised when Dropbox shared-folder ACLs cannot be inventoried safely."""


class DropboxSharedLinksCursorResetError(RuntimeError):
    """Raised when Dropbox repeatedly invalidates a shared-links cursor."""


class DropboxSharedFoldersCursorResetError(RuntimeError):
    """Raised when Dropbox repeatedly invalidates a shared-folders cursor."""


class DropboxContentPermissionError(RuntimeError):
    """Raised when the Dropbox app cannot read file content."""


class DropboxExtractionInfrastructureError(RuntimeError):
    """Raised for Riviera failures that should stop the whole sync."""


class DropboxFilePropertiesError(RuntimeError):
    """Raised when Dropbox File Properties cannot be listed safely."""


class DropboxClient:
    def __init__(
        self,
        config: dict[str, Any],
        *,
        sleeper: Callable[[float], None] = sleep,
        monotonic_clock: Callable[[], float] = monotonic,
    ) -> None:
        common_kwargs = {
            "max_retries_on_error": 5,
            "max_retries_on_rate_limit": 5,
        }

        self._client = build_dropbox_client(config, **common_kwargs)
        self._context_scope = effective_context_key(config, self._client).as_state_scope()
        self._sleeper = sleeper
        self._monotonic_clock = monotonic_clock

    def context_scope(self) -> dict[str, Any]:
        return dict(self._context_scope)

    def current_account(self) -> Any:
        try:
            return self._client.users_get_current_account()
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc)
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited the connection check.") from exc

    def list_folder(self, path: str, recursive: bool, include_deleted: bool) -> DropboxPage:
        try:
            result = self._client.files_list_folder(
                path=path,
                recursive=recursive,
                include_deleted=include_deleted,
            )
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc)
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited folder synchronization.") from exc
        return self._to_page(result)

    def list_folder_with_property_groups(self, path: str, recursive: bool) -> DropboxPage:
        """List live folder entries with all attached Dropbox File Properties."""
        try:
            result = self._client.files_list_folder(
                path=path,
                recursive=recursive,
                include_deleted=False,
                include_property_groups=TemplateFilter.filter_none,
            )
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc)
        except RateLimitError as exc:
            raise DropboxRateLimitError(
                "Dropbox rate limited file-property synchronization."
            ) from exc
        except ApiError as exc:
            raise DropboxFilePropertiesError(
                "Dropbox could not list files with File Properties."
            ) from exc
        return self._to_page(result)

    def list_folder_continue(self, cursor: str) -> DropboxPage:
        try:
            result = self._client.files_list_folder_continue(cursor)
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc)
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

    def iter_entries_with_property_groups(
        self,
        *,
        path: str,
        recursive: bool,
    ) -> Iterator[DropboxPage]:
        page = self.list_folder_with_property_groups(path, recursive)
        while True:
            yield page
            if not page.has_more:
                break
            page = self.list_folder_continue(page.cursor)

    def get_property_template(self, template_id: str) -> GetTemplateResult | None:
        try:
            return self._client.file_properties_templates_get_for_user(template_id)
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc)
        except RateLimitError as exc:
            raise DropboxRateLimitError(
                "Dropbox rate limited file-property template synchronization."
            ) from exc
        except ApiError:
            # Property values remain valid even when the human-readable template
            # schema is unavailable. The stream emits a null template_name.
            return None

    def iter_shared_links(self) -> Iterator[SharedLinksPage]:
        """List the authenticated account's shared-link inventory."""
        cursor: str | None = None
        reset_recovered = False
        while True:
            try:
                result = self._client.sharing_list_shared_links(cursor=cursor)
            except (AuthError, BadInputError) as exc:
                self._raise_auth_or_refresh_error(exc, required_scope="sharing.read")
            except RateLimitError as exc:
                raise DropboxRateLimitError(
                    "Dropbox rate limited sharing synchronization."
                ) from exc
            except ApiError as exc:
                if isinstance(exc.error, ListSharedLinksError) and exc.error.is_reset():
                    if reset_recovered:
                        raise DropboxSharedLinksCursorResetError(
                            "Dropbox repeatedly invalidated the shared-link pagination cursor."
                        ) from exc
                    # A full refresh can safely restart. Previously emitted links may replay,
                    # and destinations deduplicate them using the URL-based link_key.
                    cursor = None
                    reset_recovered = True
                    continue
                raise
            page = self._to_shared_links_page(result)
            yield page
            if not page.has_more:
                break
            cursor = page.cursor
            if cursor is None:
                raise RuntimeError("Dropbox returned shared-link pagination without a cursor.")

    def iter_shared_folders(self) -> Iterator[SharedFoldersPage]:
        """List all shared folders available to the authenticated account."""
        reset_recovered = False
        result = self._list_shared_folders()
        while True:
            page = self._to_shared_folders_page(result)
            yield page
            if page.cursor is None:
                break
            try:
                result = self._client.sharing_list_folders_continue(page.cursor)
            except (AuthError, BadInputError) as exc:
                self._raise_auth_or_refresh_error(exc, required_scope="sharing.read")
            except RateLimitError as exc:
                raise DropboxRateLimitError(
                    "Dropbox rate limited sharing synchronization."
                ) from exc
            except ApiError as exc:
                is_invalid_cursor = isinstance(
                    exc.error, ListFoldersContinueError
                ) and exc.error.is_invalid_cursor()
                if is_invalid_cursor:
                    if reset_recovered:
                        raise DropboxSharedFoldersCursorResetError(
                            "Dropbox repeatedly invalidated the shared-folder pagination cursor."
                        ) from exc
                    # A full refresh can restart safely. The shared_folder_id primary key
                    # lets destinations deduplicate entries replayed from the first page.
                    result = self._list_shared_folders()
                    reset_recovered = True
                else:
                    raise

    def iter_shared_folder_members(
        self, shared_folder_id: str
    ) -> Iterator[SharedFolderMembersPage]:
        """List all user/group/invitee members for one shared folder."""
        try:
            result = self._client.sharing_list_folder_members(shared_folder_id)
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc, required_scope="sharing.read")
        except RateLimitError as exc:
            raise DropboxRateLimitError(
                "Dropbox rate limited shared-folder membership synchronization."
            ) from exc
        except ApiError as exc:
            raise DropboxSharingAclError(
                f"Dropbox could not list members for shared folder {shared_folder_id}."
            ) from exc

        while True:
            page = self._to_shared_folder_members_page(result)
            yield page
            if page.cursor is None:
                break
            try:
                result = self._client.sharing_list_folder_members_continue(page.cursor)
            except (AuthError, BadInputError) as exc:
                self._raise_auth_or_refresh_error(exc, required_scope="sharing.read")
            except RateLimitError as exc:
                raise DropboxRateLimitError(
                    "Dropbox rate limited shared-folder membership synchronization."
                ) from exc
            except ApiError as exc:
                if isinstance(exc.error, ListFolderMembersContinueError):
                    raise DropboxSharingAclError(
                        f"Dropbox could not continue listing members for shared folder "
                        f"{shared_folder_id}."
                    ) from exc
                raise DropboxSharingAclError(
                    f"Dropbox could not continue listing members for shared folder "
                    f"{shared_folder_id}."
                ) from exc

    def _list_shared_folders(self) -> ListFoldersResult:
        try:
            return self._client.sharing_list_folders()
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc, required_scope="sharing.read")
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited sharing synchronization.") from exc

    def extract_markdown(self, file_id: str, timeout_seconds: int) -> MarkdownExtraction:
        """Convert a Dropbox file to Markdown through Riviera's asynchronous API."""
        try:
            launch = self._client.riviera_get_markdown_async(
                file_id_or_url=FileIdOrUrl.file_id(file_id),
                enable_ocr=False,
                embed_images=False,
            )
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc, required_scope="files.content.read")
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited content extraction.") from exc
        except ApiError as exc:
            raise DropboxExtractionInfrastructureError(
                "Riviera could not launch extraction."
            ) from exc

        if not getattr(launch, "is_async_job_id", lambda: False)():
            raise DropboxExtractionInfrastructureError(
                "Riviera returned an invalid extraction launch."
            )

        deadline = self._monotonic_clock() + timeout_seconds
        delay = 1.0
        job_id = launch.get_async_job_id()
        while True:
            if self._monotonic_clock() >= deadline:
                return MarkdownExtraction(
                    markdown=None,
                    extraction_status="timed_out",
                    error_type="timeout",
                    error_message=f"Riviera extraction exceeded {timeout_seconds} seconds.",
                )
            result = self._check_markdown_job(job_id)
            if not isinstance(result, GetMarkdownAsyncCheckResult):
                raise DropboxExtractionInfrastructureError(
                    "Riviera returned an invalid extraction status."
                )
            if result.is_complete():
                markdown = result.get_complete().markdown
                if not isinstance(markdown, str):
                    raise DropboxExtractionInfrastructureError(
                        "Riviera returned an invalid Markdown result."
                    )
                return MarkdownExtraction(markdown=markdown, extraction_status="succeeded")
            if result.is_failed():
                return self._normalize_markdown_failure(result.get_failed())
            if result.is_other():
                return MarkdownExtraction(
                    markdown=None,
                    extraction_status="failed",
                    error_type="unknown_status",
                    error_message="Riviera returned an unknown extraction status.",
                )
            if not result.is_in_progress():
                raise DropboxExtractionInfrastructureError(
                    "Riviera returned an invalid extraction status."
                )

            remaining = deadline - self._monotonic_clock()
            if remaining <= 0:
                return MarkdownExtraction(
                    markdown=None,
                    extraction_status="timed_out",
                    error_type="timeout",
                    error_message=f"Riviera extraction exceeded {timeout_seconds} seconds.",
                )
            self._sleeper(min(delay, remaining))
            delay = min(delay * 2, 10.0)

    def _check_markdown_job(self, job_id: str) -> GetMarkdownAsyncCheckResult:
        try:
            return self._client.riviera_get_markdown_async_check(job_id)
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc, required_scope="files.content.read")
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited content extraction.") from exc
        except ApiError as exc:
            raise DropboxExtractionInfrastructureError(
                "Riviera could not check extraction status."
            ) from exc

    @staticmethod
    def _normalize_markdown_failure(error: Any) -> MarkdownExtraction:
        error_code = getattr(error.error_code, "_tag", None)
        details = getattr(error, "error_details", None)
        error_type = getattr(details, "_tag", None)
        file_error_types = {
            "unsupported_format_error",
            "limit_exceeded_error",
            "conversion_failure_error",
            "not_found_error",
            "is_a_folder_error",
            "user_error",
        }
        if error_type in file_error_types:
            normalized_details: dict[str, Any] = {"type": error_type}
            error_message = f"Riviera extraction failed: {error_type}."
            if error_type == "user_error":
                message = details.get_user_error()
                if isinstance(message, str):
                    normalized_details["message"] = message
                    error_message = message
            return MarkdownExtraction(
                markdown=None,
                extraction_status="failed",
                error_type=error_type,
                error_code=error_code,
                error_details=normalized_details,
                error_message=error_message,
            )

        if error_code in {
            "access_error",
            "ratelimit_error",
            "unavailable",
            "api_error",
            "bad_request",
            "unknown_error",
            "other",
        }:
            raise DropboxExtractionInfrastructureError(
                f"Riviera extraction failed with systemic error: {error_code}."
            )
        raise DropboxExtractionInfrastructureError(
            "Riviera returned an unexpected extraction error."
        )

    @staticmethod
    def _authentication_message(exc: AuthError) -> str:
        tag = getattr(exc.error, "_tag", None)
        if tag == "missing_scope":
            return (
                "Dropbox credentials are valid but missing a required base scope "
                "(account_info.read or files.metadata.read)."
            )
        if tag in {"invalid_access_token", "expired_access_token"}:
            return (
                "Dropbox refresh token or access token is invalid, expired, or revoked. "
                "Re-authorize the app."
            )
        return "Dropbox rejected the supplied credentials. Check the app key and refresh token."

    @staticmethod
    def _token_exchange_message(exc: BadInputError) -> str:
        message = exc.message.lower()
        if "invalid_client" in message:
            return "Dropbox rejected the app key. Check the configured Dropbox app key."
        if "invalid_grant" in message:
            return (
                "Dropbox rejected the refresh token. It may be invalid or revoked; "
                "re-authorize the app."
            )
        return "Dropbox could not refresh the access token. Check the app key and refresh token."

    @classmethod
    def _raise_auth_or_refresh_error(
        cls, exc: AuthError | BadInputError, *, required_scope: str | None = None
    ) -> None:
        """Raise user-actionable credential errors at every Dropbox SDK boundary.

        Missing optional scopes are intentionally local to the stream that needs them.
        Refresh failures, including ones raised after connection checking, are always
        reported as connection credential failures rather than raw SDK exceptions.
        """
        if required_scope and cls._is_missing_scope_error(exc, required_scope):
            if required_scope == "sharing.read":
                raise DropboxSharingPermissionError(
                    "Dropbox app requires sharing.read to sync sharing streams."
                ) from exc
            if required_scope == "files.content.read":
                raise DropboxContentPermissionError(
                    "Dropbox app requires files.content.read to sync file_contents."
                ) from exc
        message = (
            cls._authentication_message(exc)
            if isinstance(exc, AuthError)
            else cls._token_exchange_message(exc)
        )
        raise DropboxAuthenticationError(message) from exc

    @staticmethod
    def _is_missing_scope_error(exc: AuthError | BadInputError, required_scope: str) -> bool:
        """Recognize both structured and plaintext missing-scope SDK responses.

        Some Dropbox endpoints, including Riviera, return a HTTP 400 plaintext
        response. The SDK exposes that response as ``BadInputError`` rather than
        the usual structured ``AuthError(missing_scope)``.
        """
        if isinstance(exc, AuthError):
            return getattr(exc.error, "_tag", None) == "missing_scope"
        message = exc.message.lower()
        return "required scope" in message and required_scope.lower() in message

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

    @staticmethod
    def _to_shared_folder_members_page(
        result: SharedFolderMembers,
    ) -> SharedFolderMembersPage:
        return SharedFolderMembersPage(
            users=list(result.users or []),
            groups=list(result.groups or []),
            invitees=list(result.invitees or []),
            cursor=result.cursor,
        )
