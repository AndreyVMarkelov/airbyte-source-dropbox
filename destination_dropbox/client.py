from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import dropbox
from dropbox.exceptions import ApiError, AuthError, BadInputError, RateLimitError
from dropbox.files import CreateFolderError, UploadError, WriteMode

from destination_dropbox.validation import ValidatedFileRecord


class DropboxAuthenticationError(RuntimeError):
    """Raised when Dropbox credentials cannot authenticate."""


class DropboxRateLimitError(RuntimeError):
    """Raised when Dropbox rate-limits connection validation."""


class DropboxWriteError(RuntimeError):
    """Raised when Dropbox cannot create folders or upload a destination record."""


class DropboxConflictError(DropboxWriteError):
    """Raised when the strict conflict policy finds an existing Dropbox item."""


class DropboxClient:
    """Dropbox authentication and small-file write boundary."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        credentials = config["credentials"]
        auth_type = credentials["auth_type"]
        common_kwargs = {"max_retries_on_error": 5, "max_retries_on_rate_limit": 5}
        if auth_type == "oauth2_pkce":
            self._client = dropbox.Dropbox(
                oauth2_refresh_token=credentials["refresh_token"],
                app_key=credentials["app_key"],
                **common_kwargs,
            )
        elif auth_type == "access_token":
            self._client = dropbox.Dropbox(
                oauth2_access_token=credentials["access_token"], **common_kwargs
            )
        else:
            raise ValueError(f"Unsupported auth_type: {auth_type}")
        self._ensured_folders: set[str] = set()

    def current_account(self) -> Any:
        try:
            return self._client.users_get_current_account()
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc)
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited the connection check.") from exc

    def upload_file(self, record: ValidatedFileRecord, conflict_policy: str) -> Any:
        """Create missing parent folders and upload a validated, bounded record."""
        self._ensure_parent_folders(record.destination_path)
        mode = WriteMode.overwrite if conflict_policy == "overwrite" else WriteMode.add
        try:
            return self._client.files_upload(
                record.content,
                record.destination_path,
                mode=mode,
                autorename=False,
                strict_conflict=conflict_policy == "fail",
            )
        except (AuthError, BadInputError) as exc:
            self._raise_auth_or_refresh_error(exc, required_scope="files.content.write")
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited file upload.") from exc
        except ApiError as exc:
            if self._is_upload_conflict(exc.error):
                raise DropboxConflictError(
                    "Dropbox found an existing item at the destination path."
                ) from exc
            raise DropboxWriteError("Dropbox failed to upload the file.") from exc

    def _ensure_parent_folders(self, destination_path: str) -> None:
        segments = destination_path.split("/")[1:-1]
        for index in range(1, len(segments) + 1):
            folder_path = "/" + "/".join(segments[:index])
            if folder_path in self._ensured_folders:
                continue
            try:
                self._client.files_create_folder_v2(folder_path, autorename=False)
            except (AuthError, BadInputError) as exc:
                self._raise_auth_or_refresh_error(exc, required_scope="files.content.write")
            except RateLimitError as exc:
                raise DropboxRateLimitError("Dropbox rate limited parent-folder creation.") from exc
            except ApiError as exc:
                if not self._is_existing_folder_conflict(exc.error):
                    raise DropboxWriteError("Dropbox failed to create a parent folder.") from exc
            self._ensured_folders.add(folder_path)

    def _raise_auth_or_refresh_error(
        self, exc: AuthError | BadInputError, *, required_scope: str | None = None
    ) -> None:
        if isinstance(exc, AuthError):
            raise DropboxAuthenticationError(self._auth_message(exc, required_scope)) from exc
        raise DropboxAuthenticationError(self._bad_input_message(exc)) from exc

    @staticmethod
    def _auth_message(exc: AuthError, required_scope: str | None = None) -> str:
        tag = getattr(exc.error, "_tag", None)
        if tag == "missing_scope":
            scope = required_scope or "account_info.read"
            return f"Dropbox credentials are missing the required {scope} scope."
        if tag in {"invalid_access_token", "expired_access_token"}:
            return "Dropbox refresh token or access token is invalid, expired, or revoked."
        return "Dropbox rejected the supplied credentials."

    @staticmethod
    def _bad_input_message(exc: BadInputError) -> str:
        message = exc.message.lower()
        if "invalid_client" in message:
            return "Dropbox rejected the configured app key."
        if "invalid_grant" in message:
            return "Dropbox rejected the refresh token. It may be invalid or revoked."
        return "Dropbox could not refresh the access token. Check the app key and refresh token."

    @staticmethod
    def _is_existing_folder_conflict(error: Any) -> bool:
        if not isinstance(error, CreateFolderError) or not error.is_path():
            return False
        reason = error.get_path()
        return reason.is_conflict() and reason.get_conflict().is_folder()

    @staticmethod
    def _is_upload_conflict(error: Any) -> bool:
        if not isinstance(error, UploadError) or not error.is_path():
            return False
        return error.get_path().reason.is_conflict()
