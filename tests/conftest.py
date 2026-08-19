from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run tests marked integration against a real Dropbox account",
    )
    parser.addoption(
        "--run-file-transfer-integration",
        action="store_true",
        default=False,
        help="run native Dropbox file-transfer acceptance tests",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    run_integration = config.getoption("--run-integration")
    run_file_transfer = config.getoption("--run-file-transfer-integration")
    for item in items:
        if "file_transfer_integration" in item.keywords and not run_file_transfer:
            item.add_marker(pytest.mark.skip(reason="pass --run-file-transfer-integration"))
        elif "integration" in item.keywords and not run_integration:
            item.add_marker(
                pytest.mark.skip(reason="pass --run-integration to run live Dropbox tests")
            )
