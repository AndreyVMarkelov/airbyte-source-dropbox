from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import dropbox
from dropbox import common
from dropbox.exceptions import AuthError, BadInputError


class DropboxContextError(ValueError):
    pass


def build_dropbox_client(config: Mapping[str, Any], **kwargs: Any) -> Any:
    credentials = config["credentials"]
    team_context = config.get("team_context") or {"mode": "none"}
    if not isinstance(team_context, Mapping):
        raise DropboxContextError("team_context must be an object.")
    mode = team_context.get("mode", "none")
    if mode == "none":
        client = _user_client(credentials, **kwargs)
    else:
        team = _team_client(credentials, **kwargs)
        if mode == "user":
            client = team.as_user(_required_string(team_context, "select_user"))
        elif mode == "admin":
            client = team.as_admin(_required_string(team_context, "select_admin"))
        else:
            raise DropboxContextError("team_context.mode must be one of: none, user, admin.")
    return _apply_path_root(client, config)


def _user_client(credentials: Mapping[str, Any], **kwargs: Any) -> dropbox.Dropbox:
    if credentials.get("auth_type") == "oauth2_pkce":
        return dropbox.Dropbox(
            oauth2_refresh_token=credentials["refresh_token"],
            app_key=credentials["app_key"],
            **kwargs,
        )
    if credentials.get("auth_type") == "access_token":
        return dropbox.Dropbox(oauth2_access_token=credentials["access_token"], **kwargs)
    raise DropboxContextError("Unsupported auth_type.")


def _team_client(credentials: Mapping[str, Any], **kwargs: Any) -> dropbox.DropboxTeam:
    if credentials.get("auth_type") == "oauth2_pkce":
        return dropbox.DropboxTeam(
            oauth2_refresh_token=credentials["refresh_token"],
            app_key=credentials["app_key"],
            **kwargs,
        )
    if credentials.get("auth_type") == "access_token":
        return dropbox.DropboxTeam(oauth2_access_token=credentials["access_token"], **kwargs)
    raise DropboxContextError("Unsupported auth_type.")


def _apply_path_root(client: Any, config: Mapping[str, Any]) -> Any:
    path_root = config.get("path_root") or {"mode": "default"}
    if not isinstance(path_root, Mapping):
        raise DropboxContextError("path_root must be an object.")
    mode = path_root.get("mode", "default")
    if mode == "default":
        return client
    if mode == "namespace_id":
        return client.with_path_root(
            common.PathRoot.namespace_id(_required_string(path_root, "namespace_id"))
        )
    try:
        root_info = client.users_get_current_account().root_info
    except (AuthError, BadInputError) as exc:
        raise DropboxContextError("Dropbox could not resolve root information.") from exc
    if mode == "home":
        if not getattr(root_info, "home_namespace_id", None):
            raise DropboxContextError("Dropbox account has no home namespace ID.")
        return client.with_path_root(common.PathRoot.home)
    if mode == "root":
        namespace_id = getattr(root_info, "root_namespace_id", None)
        if not namespace_id:
            raise DropboxContextError("Dropbox account has no root namespace ID.")
        return client.with_path_root(common.PathRoot.root(namespace_id))
    raise DropboxContextError("path_root.mode must be one of: default, home, root, namespace_id.")


def _required_string(config: Mapping[str, Any], field: str) -> str:
    value = config.get(field)
    if not isinstance(value, str) or not value:
        raise DropboxContextError(f"{field} must be a non-empty string.")
    return value
