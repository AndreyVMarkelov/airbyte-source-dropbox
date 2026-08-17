from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from airbyte_cdk.models import SyncMode

from source_dropbox.normalizer import normalize_shared_folder
from source_dropbox.streams.base import DropboxStream


class SharedFolders(DropboxStream):
    """Full shared-folder inventory for the authenticated Dropbox account."""

    name = "shared_folders"
    primary_key = "shared_folder_id"

    @property
    def supports_incremental(self) -> bool:
        return False

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        for page in self.client.iter_shared_folders():
            yield from (normalize_shared_folder(folder) for folder in page.entries)
