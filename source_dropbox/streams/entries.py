from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping
from typing import Any

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import CheckpointMixin

from source_dropbox.normalizer import normalize_entry
from source_dropbox.streams.base import DropboxStream


class Entries(DropboxStream, CheckpointMixin):
    name = "entries"
    primary_key = "entry_key"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._state: MutableMapping[str, Any] = {}
        self._pages: dict[str, list[Any]] = {}

    @property
    def cursor_field(self) -> list[str]:
        # The cursor is connector state only; it is never added to a destination
        # record or to the public stream schema.
        return ["cursor"]

    @property
    def supports_incremental(self) -> bool:
        return True

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._state = dict(value or {})

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        cursor = self.state.get("cursor") if sync_mode == SyncMode.incremental else None

        for page in self.client.iter_entries(
            path=self.config.get("path", ""),
            recursive=self.config.get("recursive", True),
            include_deleted=self.config.get("include_deleted", True),
            cursor=cursor,
        ):
            # Keep Dropbox SDK metadata out of slice logs. The opaque cursor lets
            # read_records retrieve exactly one complete Dropbox page.
            self._pages[page.cursor] = page.entries
            yield {"cursor": page.cursor}
            if self.state.get("cursor") != page.cursor:
                # The CDK can stop consuming a slice when an internal record limit
                # is reached. Do not advance to a later Dropbox page unless this
                # page completed and updated its state.
                return

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_slice: Mapping[str, Any] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        if not stream_slice:
            return

        page_cursor = stream_slice["cursor"]
        for entry in self._pages.pop(page_cursor):
            yield normalize_entry(entry)

        # The CDK observes this state and emits a checkpoint after read_records()
        # finishes for the slice. A Dropbox cursor therefore never advances until
        # every record in its page has been yielded.
        self.state = {"cursor": page_cursor}
