from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from airbyte_cdk.models import SyncMode

from source_dropbox.normalizer import normalize_entry
from source_dropbox.streams.base import DropboxStream


class Entries(DropboxStream):
    name = "entries"
    primary_key = "entry_key"

    @property
    def cursor_field(self) -> list[str]:
        # Dropbox's cursor represents a page boundary rather than a field returned in
        # file metadata. Attach it to each record so the CDK can checkpoint it.
        return ["cursor"]

    @property
    def supports_incremental(self) -> bool:
        return True

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        state = dict(stream_state or {})
        cursor = state.get("cursor") if sync_mode == SyncMode.incremental else None

        for page in self.client.iter_entries(
            path=self.config.get("path", ""),
            recursive=self.config.get("recursive", True),
            include_deleted=self.config.get("include_deleted", True),
            cursor=cursor,
        ):
            for entry in page.entries:
                record = normalize_entry(entry)
                record["cursor"] = page.cursor
                yield record

    def get_updated_state(
        self,
        current_stream_state: Mapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        cursor = latest_record.get("cursor")
        return {"cursor": cursor} if cursor else dict(current_stream_state or {})
