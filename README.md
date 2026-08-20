# Airbyte Dropbox

Dropbox connectors for Airbyte, packaged together for shared development and release tooling.

## Connectors

- `source_dropbox` provides Dropbox metadata, change, sharing, and optional Markdown-extraction streams:
  - `entries` is the canonical incremental change stream for files, folders, and deletions.
  - `files` and `folders` are current metadata snapshots.
  - `shared_links` and `shared_folders` are sharing snapshots.
  - `file_contents` is opt-in Markdown extraction through Dropbox Riviera.
- `destination_dropbox` writes validated file records to Dropbox, creating missing parent folders beneath its configured root.
- `source-dropbox-files` is a separate native Airbyte File Transfer connector for original Dropbox bytes. It requires Airbyte platform 1.7 or newer and is the migration path for a future file-reference-aware destination.
- `destination-dropbox-files` consumes native Airbyte file references and streams staged files to Dropbox upload sessions.

## Source Dropbox authentication

Create a scoped Dropbox app in the [Dropbox App Console](https://www.dropbox.com/developers/apps). Core streams require `account_info.read` and `files.metadata.read`; sharing streams additionally require `sharing.read`; `file_contents` additionally requires `files.content.read`.

For production, create a refresh token with the PKCE helper and copy its generated App Key and Refresh Token values into the Airbyte UI:

```bash
python -m source_dropbox.oauth authorize --app-key <APP_KEY>
```

The helper defaults to all connector scopes. Use `--scope-preset core` or `--scope-preset core+sharing` to request less. Use `--scope-preset migration` for native Dropbox-to-Dropbox file transfer; it requests content read and write scopes. Connection checking requires only core credentials; missing optional scopes fail only when their corresponding streams are selected. An access token remains available as an explicit development/manual option.

## Native file-transfer acceptance test

The opt-in end-to-end profile pipes `source-dropbox-files` native file references directly to `destination-dropbox-files`. It expects the configured source root to contain `small.bin` and `nested/large-65mb.bin`; the latter is intentionally above the old 64 MiB in-memory limit. Set `DROPBOX_TRANSFER_SOURCE_CONFIG` and `DROPBOX_TRANSFER_DESTINATION_CONFIG` to local JSON configuration files, then run:

```bash
uv run pytest --run-file-transfer-integration tests/file_transfer/integration
```

The test uses source and destination credentials independently, creates a UUID child beneath the configured destination root, and deletes only that child in `finally`. It verifies nested paths, byte-for-byte SHA-256 equality, overwrite replay, and that strict `fail` conflict policy does not release a state message. It never writes credentials to the repository.

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

The destination accepts non-empty relative POSIX paths only and resolves them below `root_path`. It rejects absolute paths, backslashes, repeated separators, and traversal segments. Content must be RFC 4648 base64 and no larger than `max_file_size_mb` after decoding (10 MiB by default; 64 MiB maximum). `sha256`, if present, is verified against the decoded content; it is not Dropbox's `content_hash`. `modified_at`, if present, must be an RFC 3339 timestamp with a timezone.

The production destination credential shape uses a Dropbox app key and refresh token. Destination connection checking requires `account_info.read` and `files.metadata.read`; uploads additionally require `files.content.write`. A non-empty `root_path` must already exist as a Dropbox folder. The destination creates only child folders below it.

`conflict_policy` defaults to `overwrite`, which makes replayed records converge on the same Dropbox bytes. `fail` stops the sync if a destination path already has a conflicting item. A `fail` upload is intentionally not replay-idempotent after an ambiguous commit failure: Dropbox may have committed a file before a lost response, and a retry can then correctly report a conflict. Use `overwrite` when Airbyte replay safety is required. The destination creates missing parent folders, uploads records in input order, and emits an Airbyte `STATE` message only after every preceding upload succeeds.

Files at or below `upload_session_threshold_mb` use Dropbox's direct upload API. Larger files use a sequential upload session: a first chunk is started, intermediate chunks are appended, and the final bytes are committed atomically. `upload_chunk_size_mb` controls the sequential chunk size (both settings default to 8 MiB). The decoded record remains in memory for this version. The connector retries transient Dropbox rate-limit/server errors per request, but does not persist upload-session IDs: a failed job replay starts a fresh session and safely overwrites the final path by default.

Raw streaming, temporary files, persisted/cross-job sessions, parallel uploads, large files beyond the configured 64 MiB ceiling, deletes/moves, timestamps, sharing, and reconciliation remain out of scope.

## Development

```bash
uv sync --all-extras
uv run ruff check .
uv run pytest
```

The default suite is mocked. The opt-in source live suite is read-only and uses the `DROPBOX_INTEGRATION_*` environment variables described in [`tests/source/integration/test_live_acceptance.py`](tests/source/integration/test_live_acceptance.py).

The opt-in destination upload test creates and deletes only a UUID-named child folder below an explicitly configured integration root. It requires `files.content.write`; the large-file verification additionally downloads the result and therefore needs `files.content.read`. Set `DROPBOX_DESTINATION_INTEGRATION_APP_KEY`, `DROPBOX_DESTINATION_INTEGRATION_REFRESH_TOKEN`, and `DROPBOX_DESTINATION_INTEGRATION_ROOT`, then run:

```bash
uv run pytest --run-integration tests/destination/integration
```

Build connector images from the repository root:

```bash
docker build -f docker/source.Dockerfile -t airbyte/source-dropbox:dev .
docker build -f docker/destination.Dockerfile -t airbyte/destination-dropbox:dev .
docker build -f docker/source-files.Dockerfile -t airbyte/source-dropbox-files:dev .
docker build -f docker/destination-files.Dockerfile -t airbyte/destination-dropbox-files:dev .
```
