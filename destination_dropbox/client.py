from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import dropbox
from dropbox.exceptions import AuthError, BadInputError, RateLimitError


class DropboxAuthenticationError(RuntimeError):
    """Raised when Dropbox credentials cannot authenticate."""


class DropboxRateLimitError(RuntimeError):
    """Raised when Dropbox rate-limits connection validation."""


class DropboxClient:
    """Authentication boundary for the future Dropbox upload implementation."""

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

    def current_account(self) -> Any:
        try:
            return self._client.users_get_current_account()
        except AuthError as exc:
            raise DropboxAuthenticationError(self._auth_message(exc)) from exc
        except BadInputError as exc:
            raise DropboxAuthenticationError(self._bad_input_message(exc)) from exc
        except RateLimitError as exc:
            raise DropboxRateLimitError("Dropbox rate limited the connection check.") from exc

    @staticmethod
    def _auth_message(exc: AuthError) -> str:
        tag = getattr(exc.error, "_tag", None)
        if tag == "missing_scope":
            return "Dropbox credentials are missing the required account_info.read scope."
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
