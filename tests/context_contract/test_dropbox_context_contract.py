from __future__ import annotations

import importlib
import importlib.util
import sys
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from source_dropbox import dropbox_context as source_context

ROOT = Path(__file__).parents[2]


ACCESS_TOKEN_CONFIG = {
    "credentials": {"auth_type": "access_token", "access_token": "test-access-token"}
}
OAUTH_CONFIG = {
    "credentials": {
        "auth_type": "oauth2_pkce",
        "app_key": "app-key",
        "refresh_token": "refresh-token",
    }
}


def test_source_context_builds_personal_access_token_client() -> None:
    with patch("source_dropbox.dropbox_context.dropbox.Dropbox") as dropbox_client:
        result = source_context.build_dropbox_client(ACCESS_TOKEN_CONFIG, max_retries=3)

    assert result is dropbox_client.return_value
    dropbox_client.assert_called_once_with(
        oauth2_access_token="test-access-token",
        max_retries=3,
    )


def test_source_context_builds_oauth_refresh_token_client() -> None:
    with patch("source_dropbox.dropbox_context.dropbox.Dropbox") as dropbox_client:
        source_context.build_dropbox_client(OAUTH_CONFIG)

    dropbox_client.assert_called_once_with(
        oauth2_refresh_token="refresh-token",
        app_key="app-key",
    )


def test_source_context_routes_team_user_and_admin() -> None:
    team = Mock()
    user_client = Mock()
    admin_client = Mock()
    team.as_user.return_value = user_client
    team.as_admin.return_value = admin_client

    with patch("source_dropbox.dropbox_context.dropbox.DropboxTeam", return_value=team):
        user = source_context.build_dropbox_client(
            {
                **ACCESS_TOKEN_CONFIG,
                "team_context": {"mode": "user", "select_user": "dbmid:user"},
            }
        )
        admin = source_context.build_dropbox_client(
            {
                **ACCESS_TOKEN_CONFIG,
                "team_context": {"mode": "admin", "select_admin": "dbmid:admin"},
            }
        )

    assert user is user_client
    assert admin is admin_client
    team.as_user.assert_called_once_with("dbmid:user")
    team.as_admin.assert_called_once_with("dbmid:admin")


@pytest.mark.parametrize(
    "config, message",
    [
        ({"team_context": {"mode": "bogus"}}, "team_context.mode"),
        ({"team_context": {"mode": "user"}}, "select_user"),
        ({"team_context": {"mode": "admin"}}, "select_admin"),
        ({"path_root": {"mode": "namespace_id"}}, "namespace_id"),
        ({"path_root": {"mode": "bogus"}}, "path_root.mode"),
    ],
)
def test_source_context_rejects_invalid_context(config: Mapping[str, object], message: str) -> None:
    base = Mock()
    base.users_get_current_account.return_value = SimpleNamespace(
        root_info=SimpleNamespace(home_namespace_id="home-ns", root_namespace_id="root-ns")
    )
    with (
        patch("source_dropbox.dropbox_context.dropbox.Dropbox", return_value=base),
        pytest.raises(source_context.DropboxContextError, match=message),
    ):
        source_context.build_dropbox_client({**ACCESS_TOKEN_CONFIG, **config})


def test_source_context_applies_all_path_root_modes() -> None:
    base = Mock(name="base")
    rooted = Mock(name="rooted")
    base.with_path_root.return_value = rooted
    base.users_get_current_account.return_value = SimpleNamespace(
        root_info=SimpleNamespace(home_namespace_id="home-ns", root_namespace_id="root-ns")
    )

    with patch("source_dropbox.dropbox_context.dropbox.Dropbox", return_value=base):
        default = source_context.build_dropbox_client(ACCESS_TOKEN_CONFIG)
        home = source_context.build_dropbox_client(
            {**ACCESS_TOKEN_CONFIG, "path_root": {"mode": "home"}}
        )
        root = source_context.build_dropbox_client(
            {**ACCESS_TOKEN_CONFIG, "path_root": {"mode": "root"}}
        )
        namespace = source_context.build_dropbox_client(
            {
                **ACCESS_TOKEN_CONFIG,
                "path_root": {"mode": "namespace_id", "namespace_id": "explicit-ns"},
            }
        )

    assert default is base
    assert home is rooted
    assert root is rooted
    assert namespace is rooted
    assert base.with_path_root.call_count == 3


@pytest.mark.parametrize(
    "config, expected",
    [
        (
            ACCESS_TOKEN_CONFIG,
            {
                "team_mode": "none",
                "path_root_mode": "default",
            },
        ),
        (
            {**ACCESS_TOKEN_CONFIG, "path_root": {"mode": "namespace_id", "namespace_id": "ns"}},
            {
                "team_mode": "none",
                "path_root_mode": "namespace_id",
                "namespace_id": "ns",
            },
        ),
        (
            {
                **ACCESS_TOKEN_CONFIG,
                "team_context": {"mode": "admin", "select_admin": "dbmid:admin"},
                "path_root": {"mode": "namespace_id", "namespace_id": "ns"},
            },
            {
                "team_mode": "admin",
                "selected_member_id": "dbmid:admin",
                "path_root_mode": "namespace_id",
                "namespace_id": "ns",
            },
        ),
    ],
)
def test_source_context_key_state_scope(
    config: Mapping[str, object], expected: dict[str, str]
) -> None:
    assert source_context.context_key(config).as_state_scope() == expected


@pytest.mark.parametrize(
    "mode, expected_namespace",
    [("home", "home-ns"), ("root", "root-ns")],
)
def test_source_effective_context_key_binds_resolved_home_root_namespace(
    mode: str, expected_namespace: str
) -> None:
    client = Mock()
    client.users_get_current_account.return_value = SimpleNamespace(
        root_info=SimpleNamespace(home_namespace_id="home-ns", root_namespace_id="root-ns")
    )

    scope = source_context.effective_context_key(
        {
            **ACCESS_TOKEN_CONFIG,
            "team_context": {"mode": "user", "select_user": "dbmid:user"},
            "path_root": {"mode": mode},
        },
        client,
    ).as_state_scope()

    assert scope == {
        "team_mode": "user",
        "selected_member_id": "dbmid:user",
        "path_root_mode": mode,
        "namespace_id": expected_namespace,
    }


def test_context_routing_is_equivalent_across_packages() -> None:
    modules = [
        source_context,
        _import_connector_module(
            "source_dropbox_files.dropbox_context",
            ROOT / "connectors" / "source-dropbox-files",
        ),
        _import_connector_module(
            "destination_dropbox_files.dropbox_context",
            ROOT / "connectors" / "destination-dropbox-files",
        ),
        _import_connector_module(
            "dropbox_repair.dropbox_context",
            ROOT / "connectors" / "dropbox-repair",
        ),
    ]
    scenarios = [
        ACCESS_TOKEN_CONFIG,
        {**ACCESS_TOKEN_CONFIG, "team_context": {"mode": "user", "select_user": "dbmid:user"}},
        {
            **ACCESS_TOKEN_CONFIG,
            "team_context": {"mode": "admin", "select_admin": "dbmid:admin"},
        },
        {
            **ACCESS_TOKEN_CONFIG,
            "path_root": {"mode": "namespace_id", "namespace_id": "ns"},
        },
        {**ACCESS_TOKEN_CONFIG, "path_root": {"mode": "home"}},
        {**ACCESS_TOKEN_CONFIG, "path_root": {"mode": "root"}},
    ]

    for module in modules:
        for config in scenarios:
            _assert_build_context_contract(module, config)


def _assert_build_context_contract(module: ModuleType, config: Mapping[str, object]) -> None:
    base = Mock(name="base")
    rooted = Mock(name="rooted")
    base.with_path_root.return_value = rooted
    base.users_get_current_account.return_value = SimpleNamespace(
        root_info=SimpleNamespace(home_namespace_id="home-ns", root_namespace_id="root-ns")
    )
    team = Mock(name="team")
    team.as_user.return_value = base
    team.as_admin.return_value = base

    with (
        patch.object(module.dropbox, "Dropbox", return_value=base) as dropbox_client,
        patch.object(module.dropbox, "DropboxTeam", return_value=team) as dropbox_team,
    ):
        result = module.build_dropbox_client(config)

    team_mode = (config.get("team_context") or {}).get("mode", "none")  # type: ignore[union-attr]
    path_root_mode = (config.get("path_root") or {}).get("mode", "default")  # type: ignore[union-attr]
    if team_mode == "none":
        dropbox_client.assert_called_once()
        dropbox_team.assert_not_called()
    elif team_mode == "user":
        dropbox_team.assert_called_once()
        team.as_user.assert_called_once_with("dbmid:user")
    elif team_mode == "admin":
        dropbox_team.assert_called_once()
        team.as_admin.assert_called_once_with("dbmid:admin")

    if path_root_mode == "default":
        assert result is base
        base.with_path_root.assert_not_called()
    else:
        assert result is rooted
        base.with_path_root.assert_called_once()


def _import_connector_module(module: str, package_root: Path) -> ModuleType:
    module_path = package_root / Path(*module.split(".")).with_suffix(".py")
    module_name = f"context_contract_{module.replace('.', '_')}"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Cannot load {module_path}")
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = loaded
    spec.loader.exec_module(loaded)
    return loaded
