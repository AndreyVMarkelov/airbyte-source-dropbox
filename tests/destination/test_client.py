from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dropbox.exceptions import AuthError, BadInputError, RateLimitError

from destination_dropbox.client import (
    DropboxAuthenticationError,
    DropboxClient,
    DropboxRateLimitError,
)


def test_refresh_token_authentication_uses_app_key() -> None:
    config = {
        "credentials": {"auth_type": "oauth2_pkce", "app_key": "key", "refresh_token": "token"}
    }
    client = DropboxClient(config)
    assert client._client is not None


def test_current_account_classifies_errors() -> None:
    client = DropboxClient.__new__(DropboxClient)
    client._client = Mock()
    client._client.users_get_current_account.side_effect = AuthError(
        "request-id", SimpleNamespace(_tag="invalid_access_token")
    )
    with pytest.raises(DropboxAuthenticationError, match="invalid, expired, or revoked"):
        client.current_account()

    client._client.users_get_current_account.side_effect = BadInputError(
        "request-id", '{"error":"invalid_client"}'
    )
    with pytest.raises(DropboxAuthenticationError, match="app key"):
        client.current_account()

    client._client.users_get_current_account.side_effect = RateLimitError("request-id")
    with pytest.raises(DropboxRateLimitError, match="rate limited"):
        client.current_account()
