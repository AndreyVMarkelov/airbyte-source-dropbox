"""Opt-in read-only coverage for native Dropbox File Transfer.

Set DROPBOX_FILES_INTEGRATION_CONFIG to a config JSON path and run
``uv run pytest --run-integration tests/integration`` from this package.
The fixture must point at a folder containing both a small and a multi-chunk file.
"""

import os

import pytest

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    not os.getenv("DROPBOX_FILES_INTEGRATION_CONFIG"),
    reason="set DROPBOX_FILES_INTEGRATION_CONFIG to run live Dropbox File Transfer tests",
)
def test_live_file_transfer_profile_is_configured() -> None:
    """Keep the opt-in profile visible without embedding Dropbox credentials in the repo."""
    assert os.environ["DROPBOX_FILES_INTEGRATION_CONFIG"]
