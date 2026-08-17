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
        # Dropbox cursors represent pages, not individual records. State is emitted
        # explicitly after each complete page in read().
        return []

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
        raise NotImplementedError("Entries emits page-level checkpoints through read().")

    def read(
        self,
        configured_stream: Any,
        logger: Any,
        slice_logger: Any,
        stream_state: Mapping[str, Any] | None = None,
        state_manager: Any = None,
        internal_config: Any = None,
    ) -> Iterable[Any]:
        state = dict(stream_state or {})
        sync_mode = configured_stream.sync_mode
        cursor = state.get("cursor") if sync_mode == SyncMode.incremental else None

        for page in self.client.iter_entries(
            path=self.config.get("path", ""),
            recursive=self.config.get("recursive", True),
            include_deleted=self.config.get("include_deleted", True),
            cursor=cursor,
        ):
            for entry in page.entries:
                yield normalize_entry(entry)

            # Dropbox cursors describe the state after an entire page. Emit the
            # checkpoint only after every record from that page has been yielded.
            state = {"cursor": page.cursor}
            if state_manager:
                yield self._checkpoint_state(state, state_manager)
