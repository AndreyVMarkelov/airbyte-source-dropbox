from __future__ import annotations

import base64
import os
from contextlib import suppress
from uuid import uuid4

import pytest
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    SyncMode,
    Type,
)
from dropbox.exceptions import ApiError

from destination_dropbox.client import DropboxClient
from destination_dropbox.destination import DestinationDropbox

pytestmark = pytest.mark.integration

REQUIRED_ENVIRONMENT = (
    "DROPBOX_DESTINATION_INTEGRATION_APP_KEY",
    "DROPBOX_DESTINATION_INTEGRATION_REFRESH_TOKEN",
    "DROPBOX_DESTINATION_INTEGRATION_ROOT",
)


def _config() -> dict[str, object]:
    missing = [name for name in REQUIRED_ENVIRONMENT if not os.environ.get(name)]
    if missing:
        pytest.fail(
            "Missing destination integration environment variables: " + ", ".join(missing)
        )
    root = os.environ["DROPBOX_DESTINATION_INTEGRATION_ROOT"].rstrip("/")
    if not root.startswith("/") or root == "":
        pytest.fail(
            "DROPBOX_DESTINATION_INTEGRATION_ROOT must be a non-empty absolute Dropbox path."
        )
    return {
        "credentials": {
            "auth_type": "oauth2_pkce",
            "app_key": os.environ["DROPBOX_DESTINATION_INTEGRATION_APP_KEY"],
            "refresh_token": os.environ["DROPBOX_DESTINATION_INTEGRATION_REFRESH_TOKEN"],
        },
        "root_path": root,
        "max_file_size_mb": 10,
        "conflict_policy": "overwrite",
    }


def _catalog() -> ConfiguredAirbyteCatalog:
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="documents", json_schema={}, supported_sync_modes=[SyncMode.full_refresh]
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.append,
            )
        ]
    )


def test_live_upload_creates_nested_parents_and_cleans_up() -> None:
    config = _config()
    test_root = f"{config['root_path']}/airbyte-destination-{uuid4().hex}"
    child_name = test_root.rsplit("/", maxsplit=1)[-1]
    record = AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(
            stream="documents",
            data={
                "path": f"{child_name}/nested/report.txt",
                "content_base64": base64.b64encode(b"airbyte destination integration").decode(),
            },
            emitted_at=0,
        ),
    )
    state = AirbyteMessage(type=Type.STATE)
    client = DropboxClient(config)

    try:
        output = list(DestinationDropbox().write(config, _catalog(), [record, state]))
        assert output == [state]
    finally:
        # The UUID child is the only path removed by this test; never remove the configured root.
        with suppress(ApiError):
            client._client.files_delete_v2(test_root)
