from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from airbyte_cdk.models import SyncMode

from source_dropbox.normalizer import normalize_shared_link
from source_dropbox.streams.base import DropboxStream


class SharedLinks(DropboxStream):
    """Full shared-link inventory for the authenticated Dropbox account."""

    name = "shared_links"
    primary_key = "link_key"

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
        for page in self.client.iter_shared_links():
            yield from (normalize_shared_link(link) for link in page.links)
