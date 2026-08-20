import copy
import hashlib
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid4

import pytest

from source_dropbox.client import DropboxClient

pytestmark = pytest.mark.file_transfer_integration

REPOSITORY_ROOT = Path(__file__).parents[3]
SOURCE_BINARY = REPOSITORY_ROOT / "connectors/source-dropbox-files/.venv/bin/source-dropbox-files"
DESTINATION_BINARY = (
    REPOSITORY_ROOT / "connectors/destination-dropbox-files/.venv/bin/destination-dropbox-files"
)
EXPECTED_FILES = ("small.bin", "nested/large-65mb.bin")


@dataclass(frozen=True)
class PipelineResult:
    source_returncode: int
    destination_returncode: int
    destination_messages: list[dict[str, Any]]


def _load_config(variable: str) -> dict[str, Any]:
    return json.loads(Path(os.environ[variable]).read_text())


def _catalog() -> dict[str, Any]:
    return {
        "streams": [
            {
                "stream": {
                    "name": "raw_files",
                    "json_schema": {"type": "object"},
                    "supported_sync_modes": ["full_refresh"],
                },
                "sync_mode": "full_refresh",
                "destination_sync_mode": "append",
            }
        ]
    }


def _run_pipeline(source_config: Path, destination_config: Path, catalog: Path) -> PipelineResult:
    """Pipe native file references directly between both connector processes."""
    source = subprocess.Popen(
        [str(SOURCE_BINARY), "read", "--config", str(source_config), "--catalog", str(catalog)],
        stdout=subprocess.PIPE,
        # Do not pipe stderr without concurrently consuming it: a connector
        # failure can otherwise fill the pipe and deadlock this acceptance test.
        stderr=subprocess.DEVNULL,
        text=True,
    )
    assert source.stdout is not None
    destination = subprocess.Popen(
        [
            str(DESTINATION_BINARY),
            "write",
            "--config",
            str(destination_config),
            "--catalog",
            str(catalog),
        ],
        stdin=source.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    source.stdout.close()
    destination_output, _ = destination.communicate(timeout=600)
    return PipelineResult(
        source_returncode=source.wait(timeout=600),
        destination_returncode=destination.returncode,
        destination_messages=[json.loads(line) for line in destination_output.splitlines() if line],
    )


def _download_sha256(client: DropboxClient, path: str) -> tuple[int, str]:
    metadata, response = client._client.files_download(path)  # noqa: SLF001 - acceptance verification only
    digest = hashlib.sha256()
    for chunk in response.iter_content(chunk_size=1024 * 1024):
        if chunk:
            digest.update(chunk)
    return metadata.size, digest.hexdigest()


@pytest.mark.skipif(
    not all(
        os.getenv(key)
        for key in ("DROPBOX_TRANSFER_SOURCE_CONFIG", "DROPBOX_TRANSFER_DESTINATION_CONFIG")
    ),
    reason="set DROPBOX_TRANSFER_SOURCE_CONFIG and DROPBOX_TRANSFER_DESTINATION_CONFIG",
)
def test_native_file_transfer_preserves_bytes_replays_and_withholds_failed_state() -> None:
    """Exercise source → destination hand-off, session upload, replay, and strict conflicts."""
    source_config_path = Path(os.environ["DROPBOX_TRANSFER_SOURCE_CONFIG"])
    source_config = _load_config("DROPBOX_TRANSFER_SOURCE_CONFIG")
    destination_config = _load_config("DROPBOX_TRANSFER_DESTINATION_CONFIG")
    source_root = source_config["path"].rstrip("/")
    configured_destination_root = destination_config["root_path"].rstrip("/")
    source_verifier = DropboxClient(source_config)
    destination_verifier = DropboxClient(destination_config)
    destination_child = f"{configured_destination_root}/airbyte-file-transfer-{uuid4()}"
    created_destination_child = False

    # The configured root is an operator-owned fixture. This test owns only
    # its UUID child and cleans it up regardless of the test outcome.
    try:
        destination_verifier._client.files_create_folder_v2(destination_child)  # noqa: SLF001
        created_destination_child = True
        with TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            catalog_path = temporary_path / "catalog.json"
            catalog_path.write_text(json.dumps(_catalog()))
            destination_config["root_path"] = destination_child
            destination_config_path = temporary_path / "destination-overwrite.json"
            destination_config_path.write_text(json.dumps(destination_config))

            # First write and replay both use overwrite and must safely converge.
            for _ in range(2):
                result = _run_pipeline(source_config_path, destination_config_path, catalog_path)
                assert result.source_returncode == 0
                assert result.destination_returncode == 0
                message_types = [message["type"] for message in result.destination_messages]
                assert message_types.count("STATE") == 1

            for relative_path in EXPECTED_FILES:
                source_size, source_sha256 = _download_sha256(
                    source_verifier, f"{source_root}/{relative_path}"
                )
                destination_size, destination_sha256 = _download_sha256(
                    destination_verifier, f"{destination_child}/{relative_path}"
                )
                assert destination_size == source_size
                assert destination_sha256 == source_sha256

            failing_config = copy.deepcopy(destination_config)
            failing_config["conflict_policy"] = "fail"
            failing_config_path = temporary_path / "destination-fail.json"
            failing_config_path.write_text(json.dumps(failing_config))

            result = _run_pipeline(source_config_path, failing_config_path, catalog_path)
            assert result.destination_returncode != 0
            assert not any(message["type"] == "STATE" for message in result.destination_messages)
    finally:
        if created_destination_child:
            destination_verifier._client.files_delete_v2(destination_child)  # noqa: SLF001
