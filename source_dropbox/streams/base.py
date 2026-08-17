from __future__ import annotations

from typing import Any

from airbyte_cdk.sources.streams import Stream

from source_dropbox.client import DropboxClient


class DropboxStream(Stream):
    def __init__(self, client: DropboxClient, config: dict[str, Any]) -> None:
        super().__init__()
        self.client = client
        self.config = config
