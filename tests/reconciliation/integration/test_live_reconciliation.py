"""Opt-in live coverage for metadata-only Dropbox reconciliation.

Both configurations must identify existing integration roots and have
``files.content.write`` temporarily available to create and clean up fixtures.
The reconciliation implementation itself uses metadata APIs only.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from dropbox.files import WriteMode

from dropbox_reconciliation.client import DropboxReconciliationClient
from dropbox_reconciliation.reconcile import reconcile, summarize

pytestmark = pytest.mark.integration


def _config(variable: str) -> dict[str, object]:
    return json.loads(Path(os.environ[variable]).read_text())


def _upload(
    client: DropboxReconciliationClient,
    path: str,
    content: bytes,
    *,
    client_modified: datetime | None = None,
) -> None:
    client._client.files_upload(  # noqa: SLF001
        content,
        path,
        mode=WriteMode.overwrite,
        client_modified=client_modified,
    )


@pytest.mark.skipif(
    not all(
        os.getenv(variable)
        for variable in (
            "DROPBOX_RECONCILIATION_SOURCE_CONFIG",
            "DROPBOX_RECONCILIATION_DESTINATION_CONFIG",
        )
    ),
    reason="set DROPBOX_RECONCILIATION_SOURCE_CONFIG and DROPBOX_RECONCILIATION_DESTINATION_CONFIG",
)
def test_live_reconciliation_uses_independent_roots_and_cleans_up() -> None:
    source_config = _config("DROPBOX_RECONCILIATION_SOURCE_CONFIG")
    destination_config = _config("DROPBOX_RECONCILIATION_DESTINATION_CONFIG")
    source_client = DropboxReconciliationClient(source_config, "source")
    destination_client = DropboxReconciliationClient(destination_config, "destination")
    source_root = source_client.validate_root(source_config.get("root_path"))
    destination_root = destination_client.validate_root(destination_config.get("root_path"))
    suffix = uuid4().hex
    source_child = f"{source_root}/airbyte-reconciliation-{suffix}"
    destination_child = f"{destination_root}/airbyte-reconciliation-{suffix}"
    source_created = False
    destination_created = False

    try:
        source_client._client.files_create_folder_v2(source_child)  # noqa: SLF001
        source_created = True
        destination_client._client.files_create_folder_v2(destination_child)  # noqa: SLF001
        destination_created = True

        migrated_timestamp = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
        _upload(
            source_client,
            f"{source_child}/matched.bin",
            b"same",
            client_modified=migrated_timestamp,
        )
        _upload(
            destination_client,
            f"{destination_child}/matched.bin",
            b"same",
            client_modified=migrated_timestamp,
        )
        _upload(source_client, f"{source_child}/missing.bin", b"source")
        _upload(destination_client, f"{destination_child}/extra.bin", b"destination")
        _upload(
            source_client,
            f"{source_child}/size.bin",
            b"longer",
            client_modified=migrated_timestamp,
        )
        _upload(
            destination_client,
            f"{destination_child}/size.bin",
            b"x",
            client_modified=migrated_timestamp,
        )
        _upload(
            source_client,
            f"{source_child}/hash.bin",
            b"source",
            client_modified=migrated_timestamp,
        )
        _upload(
            destination_client,
            f"{destination_child}/hash.bin",
            b"target",
            client_modified=migrated_timestamp,
        )

        records = reconcile(
            source_client.inventory(source_child), destination_client.inventory(destination_child)
        )

        assert [(record.path, record.status, record.reason) for record in records] == [
            ("extra.bin", "extra_destination", "destination_only"),
            ("hash.bin", "mismatched", "content_hash_mismatch"),
            ("matched.bin", "matched", "matched"),
            ("missing.bin", "missing", "source_only"),
            ("size.bin", "mismatched", "size_mismatch"),
        ]
        assert summarize(records) == {
            "type": "summary",
            "total_paths": 5,
            "matched": 1,
            "missing": 1,
            "mismatched": 2,
            "extra_destination": 1,
            "errors": 0,
            "metadata_mismatches": {"client_modified": 0},
        }
    finally:
        if source_created:
            source_client._client.files_delete_v2(source_child)  # noqa: SLF001
        if destination_created:
            destination_client._client.files_delete_v2(destination_child)  # noqa: SLF001
