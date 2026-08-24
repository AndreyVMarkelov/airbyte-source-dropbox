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
        self._context_scope = _client_context_scope(self.client)

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
        incoming = dict(value or {})
        context = incoming.get("context")
        if context is not None and context != self._context_scope:
            raise ValueError("Dropbox entries state context does not match the configured root.")
        self._state = incoming

    def _cursor_for_sync(self, sync_mode: SyncMode) -> str | None:
        """Return a saved cursor only for an incremental change sync.

        A full refresh is always a new snapshot from the configured root. It may
        emit page checkpoints for Airbyte during that job, but it must never use a
        prior full-refresh checkpoint as a Dropbox change cursor.
        """
        if sync_mode == SyncMode.incremental:
            if _requires_context_scope(self._context_scope):
                context = self.state.get("context")
                if context is None and self.state.get("cursor"):
                    raise ValueError(
                        "Dropbox entries state has no Business/Path Root context; reset state "
                        "before using this configured root."
                    )
            return self.state.get("cursor")
        return None

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: list[str] | None = None,
        stream_state: Mapping[str, Any] | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        cursor = self._cursor_for_sync(sync_mode)

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
        entries = self._pages[page_cursor]
        try:
            for entry in entries:
                yield normalize_entry(entry)

            # The CDK observes this state and emits a checkpoint after
            # read_records() finishes for the slice. A Dropbox cursor therefore
            # never advances until every record in its page has been yielded.
            state = {"cursor": page_cursor}
            if _requires_context_scope(self._context_scope):
                state["context"] = self._context_scope
            self.state = state
        finally:
            # A normalization or downstream read failure must not retain Dropbox
            # metadata in memory for the lifetime of the stream instance.
            self._pages.pop(page_cursor, None)


def _requires_context_scope(context: Mapping[str, Any]) -> bool:
    return context != {
        "team_mode": "none",
        "path_root_mode": "default",
    }


def _client_context_scope(client: Any) -> dict[str, Any]:
    try:
        context = client.context_scope()
    except AttributeError:
        context = None
    if isinstance(context, Mapping):
        return dict(context)
    return {"team_mode": "none", "path_root_mode": "default"}
