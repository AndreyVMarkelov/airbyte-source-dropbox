from __future__ import annotations

import os
from typing import Any

import pytest

REQUIRED_ENVIRONMENT = (
    "DROPBOX_INTEGRATION_APP_KEY",
    "DROPBOX_INTEGRATION_REFRESH_TOKEN_CORE",
    "DROPBOX_INTEGRATION_REFRESH_TOKEN_SHARING",
    "DROPBOX_INTEGRATION_REFRESH_TOKEN_CONTENT",
    "DROPBOX_INTEGRATION_TEST_PATH",
)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run tests marked integration against a real Dropbox account",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--run-integration"):
        return
    skip = pytest.mark.skip(reason="pass --run-integration to run live Dropbox tests")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def integration_environment(request: pytest.FixtureRequest) -> dict[str, str]:
    if not request.config.getoption("--run-integration"):
        pytest.skip("pass --run-integration to run live Dropbox tests")
    missing = [name for name in REQUIRED_ENVIRONMENT if not os.environ.get(name)]
    if missing:
        pytest.fail(f"Missing live Dropbox integration environment variables: {', '.join(missing)}")
    return {name: os.environ[name] for name in REQUIRED_ENVIRONMENT}


def refresh_config(environment: dict[str, str], token_name: str) -> dict[str, Any]:
    return {
        "credentials": {
            "auth_type": "oauth2_pkce",
            "app_key": environment["DROPBOX_INTEGRATION_APP_KEY"],
            "refresh_token": environment[token_name],
        },
        "path": environment["DROPBOX_INTEGRATION_TEST_PATH"],
        "recursive": True,
        "include_deleted": True,
        "file_contents": {
            "allowed_extensions": [".pdf", ".docx"],
            "max_file_size_mb": 1,
            "timeout_seconds": 300,
        },
    }


@pytest.fixture(scope="session")
def core_config(integration_environment: dict[str, str]) -> dict[str, Any]:
    return refresh_config(integration_environment, "DROPBOX_INTEGRATION_REFRESH_TOKEN_CORE")


@pytest.fixture(scope="session")
def sharing_config(integration_environment: dict[str, str]) -> dict[str, Any]:
    return refresh_config(integration_environment, "DROPBOX_INTEGRATION_REFRESH_TOKEN_SHARING")


@pytest.fixture(scope="session")
def content_config(integration_environment: dict[str, str]) -> dict[str, Any]:
    return refresh_config(integration_environment, "DROPBOX_INTEGRATION_REFRESH_TOKEN_CONTENT")


@pytest.fixture(scope="session")
def integration_secrets(integration_environment: dict[str, str]) -> set[str]:
    """Credential values only; fixture paths are expected in Dropbox records."""
    return {
        integration_environment["DROPBOX_INTEGRATION_APP_KEY"],
        integration_environment["DROPBOX_INTEGRATION_REFRESH_TOKEN_CORE"],
        integration_environment["DROPBOX_INTEGRATION_REFRESH_TOKEN_SHARING"],
        integration_environment["DROPBOX_INTEGRATION_REFRESH_TOKEN_CONTENT"],
    }
