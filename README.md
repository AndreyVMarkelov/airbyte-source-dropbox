# Airbyte Dropbox

Dropbox connectors for Airbyte, packaged together for shared development and release tooling.

## Connectors

- `source_dropbox` provides Dropbox metadata, change, sharing, and optional Markdown-extraction streams:
  - `entries` is the canonical incremental change stream for files, folders, and deletions.
  - `files` and `folders` are current metadata snapshots.
  - `shared_links` and `shared_folders` are sharing snapshots.
  - `file_contents` is opt-in Markdown extraction through Dropbox Riviera.
- `destination_dropbox` writes validated file records to Dropbox, creating missing parent folders beneath its configured root.
- `source-dropbox-files` is a separate native Airbyte File Transfer connector for original Dropbox bytes. It requires Airbyte platform 1.7 or newer, carries source `client_modified` plus provenance-only `server_modified`, and uses Dropbox list-folder cursors for incremental runs after the initial snapshot.
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

## Dropbox-native reconciliation

`dropbox-reconciliation` is a read-only CLI that compares two Dropbox folder roots using normalized relative paths, file size, and Dropbox's `content_hash`. Dropbox `content_hash` is not SHA-256; reconciliation never downloads file bytes or falls back to size-only matching.

Top-level `status` and `reason` retain their original content meaning: `matched`, `missing`, `mismatched`, `extra_destination`, or `error`. The report also includes richer read-only dimensions:

- `content` repeats the authoritative size/content-hash comparison.
- `namespace` reports the conservative path/namespace comparison. This version does not guess migrated identity from matching content.
- `metadata.client_modified` compares UTC-normalized client-modified instants when both sides provide them. A timestamp mismatch does not change top-level content status. `server_modified` is included only as Dropbox-owned provenance and is never treated as a fidelity mismatch.

The summary keeps the original content counters and adds `total_paths` and `metadata_mismatches.client_modified`. Datetime fields are emitted as UTC strings with a `Z` suffix.

Both sides require independent credentials with `account_info.read` and `files.metadata.read`:

```json
{
  "source": {"credentials": {"auth_type": "oauth2_pkce", "app_key": "...", "refresh_token": "..."}, "root_path": "/migration-source"},
  "destination": {"credentials": {"auth_type": "oauth2_pkce", "app_key": "...", "refresh_token": "..."}, "root_path": "/migration-destination"}
}
```

```bash
dropbox-reconciliation compare --config /path/reconciliation.json > report.jsonl
```

Records are sorted by normalized relative path; the final JSONL line is a summary. A completed comparison exits successfully even when it finds `missing`, `mismatched`, or `extra_destination` files. Invalid roots, credentials, pagination failures, and duplicate normalized paths fail the command because they make the report incomplete or ambiguous.

The opt-in live reconciliation profile uses separate source and destination config paths and creates only UUID children below their configured roots. It requires `files.content.write` only to create and remove those test fixtures:

```bash
DROPBOX_RECONCILIATION_SOURCE_CONFIG=/path/source.json \
DROPBOX_RECONCILIATION_DESTINATION_CONFIG=/path/destination.json \
uv run pytest --run-integration tests/reconciliation/integration
```

## Shared-link inventory

`shared_links` is a read-only full-refresh snapshot of Dropbox shared links
visible to the authenticated account. It requires `sharing.read`, uses
`sharing_list_shared_links`, paginates every page, and never downloads file
content or mutates sharing state.

The stream primary key remains `link_key`. Dropbox does not expose a durable
shared-link ID for every SDK link variant, so `link_key` and `link_id` use the
canonical shared-link URL. Treat shared-link URLs as sensitive data.

Records preserve Dropbox sharing semantics without inferring exposure from the
URL. Common warehouse fields include `target.type`, `target.id`,
`target.path_lower`, `visibility`, `access_level`, `settings.effective_visibility`,
`settings.requested_visibility`, `settings.link_access_level`, `settings.allow_download`,
and `expires`. File links include file metadata when Dropbox returns it, such as
`rev`, `client_modified`, `server_modified`, and `size`; folder links keep a
folder-shaped target instead of file-only fields.

The stream honors the configured Dropbox `path` using case-insensitive
path-component matching. Links without a safe target path, or whose target is
outside the configured root, are skipped with a safe warning.

Example warehouse predicates:

```sql
-- Expiring links.
select url, target.path_lower, expires
from shared_links
where expires is not null;

-- Links Dropbox reports as effectively public and downloadable.
select url, target.type, target.path_lower
from shared_links
where settings.effective_visibility = 'public'
  and settings.allow_download = true;
```

## Targeted repair

`dropbox-repair` consumes a completed reconciliation JSONL report and acts only on `missing` and `mismatched` source records. It streams the source file by stable file ID into an overwrite upload session; `matched`, `extra_destination`, and `error` records are reported as skipped. It never deletes destination-only files.

```bash
dropbox-repair apply \
  --report reconciliation.jsonl \
  --source-config /path/source.json \
  --destination-config /path/destination.json
```

The tool validates the entire report before the first mutation, validates every relative path below both configured roots, and checks source revision, size, and Dropbox `content_hash` again before transfer. It emits JSONL only after each durable upload. A malformed report or upload failure stops the run; later files are not claimed as repaired.

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

`metadata_policy` defaults to `preserve`: a supplied `modified_at` is normalized to UTC and sent to Dropbox as `client_modified`. Set it to `ignore` to let Dropbox assign its default client-modified timestamp. Dropbox owns `server_modified`; neither destination tries to recreate it. Source file IDs, revisions, and Dropbox `content_hash` remain source provenance and are never supplied as destination write identity.

The production destination credential shape uses a Dropbox app key and refresh token. Destination connection checking requires `account_info.read` and `files.metadata.read`; uploads additionally require `files.content.write`. A non-empty `root_path` must already exist as a Dropbox folder. The destination creates only child folders below it.

`conflict_policy` defaults to `overwrite`, which makes replayed records converge on the same Dropbox bytes. `fail` stops the sync if a destination path already has a conflicting item. A `fail` upload is intentionally not replay-idempotent after an ambiguous commit failure: Dropbox may have committed a file before a lost response, and a retry can then correctly report a conflict. Use `overwrite` when Airbyte replay safety is required. The destination creates missing parent folders, uploads records in input order, and emits an Airbyte `STATE` message only after every preceding upload succeeds.

Files at or below `upload_session_threshold_mb` use Dropbox's direct upload API. Larger files use a sequential upload session: a first chunk is started, intermediate chunks are appended, and the final bytes are committed atomically. `upload_chunk_size_mb` controls the sequential chunk size (both settings default to 8 MiB). The decoded record remains in memory for this version. The connector retries transient Dropbox rate-limit/server errors per request, but does not persist upload-session IDs: a failed job replay starts a fresh session and safely overwrites the final path by default.

Raw streaming, temporary files, persisted/cross-job sessions, parallel uploads, large files beyond the configured 64 MiB ceiling, sharing, and reconciliation remain out of scope for the base64-record destination.

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
