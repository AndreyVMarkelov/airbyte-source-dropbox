# Airbyte Dropbox

Dropbox connectors for Airbyte, packaged together for shared development and release tooling.

## Connectors

- `source_dropbox` provides Dropbox metadata, change, sharing, and optional Markdown-extraction streams:
  - `entries` is the canonical incremental change stream for files, folders, and deletions.
  - `files` and `folders` are current metadata snapshots.
  - `shared_links` and `shared_folders` are sharing snapshots.
  - `file_contents` is opt-in Markdown extraction through Dropbox Riviera.
- `destination_dropbox` defines the Dropbox file-write contract. It authenticates and validates input records, but does **not** upload or otherwise mutate Dropbox in this first release.

## Source Dropbox authentication

Create a scoped Dropbox app in the [Dropbox App Console](https://www.dropbox.com/developers/apps). Core streams require `account_info.read` and `files.metadata.read`; sharing streams additionally require `sharing.read`; `file_contents` additionally requires `files.content.read`.

For production, create a refresh token with the PKCE helper and copy its generated App Key and Refresh Token values into the Airbyte UI:

```bash
python -m source_dropbox.oauth authorize --app-key <APP_KEY>
```

The helper defaults to all connector scopes. Use `--scope-preset core` or `--scope-preset core+sharing` to request less. Connection checking requires only core credentials; missing optional scopes fail only when their corresponding streams are selected. An access token remains available as an explicit development/manual option.

`entries` keeps Dropbox cursor state internal. Incremental jobs resume from the stored cursor; full refresh jobs always start at the configured root, even when an earlier state is supplied.

## Destination file-write contract

```json
{
  "path": "folder/report.pdf",
  "content_base64": "...",
  "sha256": "optional lowercase SHA-256 digest",
  "modified_at": "2026-08-18T12:00:00Z"
}
```

The destination accepts non-empty relative POSIX paths only and resolves them below `root_path`. It rejects absolute paths, backslashes, repeated separators, and traversal segments. Content must be RFC 4648 base64 and no larger than `max_file_size_mb` after decoding (10 MiB by default). `sha256`, if present, is verified against the decoded content; it is not Dropbox's `content_hash`. `modified_at`, if present, must be an RFC 3339 timestamp with a timezone.

The production destination credential shape uses a Dropbox app key and refresh token. Future uploads require `files.content.write`; current connection checking requires `account_info.read`.

## Development

```bash
uv sync --all-extras
uv run ruff check .
uv run pytest
```

The default suite is mocked. The opt-in source live suite is read-only and uses the `DROPBOX_INTEGRATION_*` environment variables described in [`tests/source/integration/test_live_acceptance.py`](tests/source/integration/test_live_acceptance.py).

Build connector images from the repository root:

```bash
docker build -f docker/source.Dockerfile -t airbyte/source-dropbox:dev .
docker build -f docker/destination.Dockerfile -t airbyte/destination-dropbox:dev .
```
