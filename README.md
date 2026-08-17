# Airbyte Source Dropbox

Airbyte source connector for Dropbox with incremental sync, metadata ingestion, and optional document extraction.

## Status

Early development.

## Streams

- `entries` — canonical change stream for files, folders, and deletions from Dropbox `list_folder`; supports incremental sync.
- `files` — current full-refresh snapshot of Dropbox file metadata.
- `folders` — current full-refresh snapshot of Dropbox folder metadata.
- `shared_links` — current full-refresh inventory of account shared links.
- `shared_folders` — current full-refresh inventory of shared folders available to the account.

## Planned streams

- `file_contents` — optional extracted text for supported document formats

## Authentication

The connector is designed to use a Dropbox refresh token with an app key. PKCE authorization tooling will be added before the first release.

`shared_links` and `shared_folders` require the Dropbox `sharing.read` scope. They are
full-refresh streams for governance, migration, and RAG ACL enrichment. The core connection
check and file/folder streams do not require this scope.

## State and sync modes

Dropbox `list_folder` cursors are internal connector state; they are not emitted in `entries` records. The connector checkpoints only after a complete Dropbox results page.

- **Incremental sync** persists and consumes the saved cursor on the next incremental job, so Dropbox returns changes since that cursor.
- **Full refresh** always starts a new snapshot at the configured root, even if Airbyte supplies a previous state. Full-refresh jobs can emit page checkpoints while running, but those checkpoints are intentionally not used as the baseline for a later full refresh.

If Dropbox invalidates an incremental cursor, the connector restarts from the configured root. This can replay existing records, so destinations should use the `entry_key` primary key for idempotent upserts/deduplication.

## Development

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest
```

Run the Airbyte commands:

```bash
python -m source_dropbox.run spec
python -m source_dropbox.run check --config secrets/config.json
python -m source_dropbox.run discover --config secrets/config.json
python -m source_dropbox.run read \
  --config secrets/config.json \
  --catalog secrets/catalog.json
```

Build and run the Docker image:

```bash
docker build -t airbyte/source-dropbox:dev .
docker run --rm airbyte/source-dropbox:dev spec
docker run --rm -v "$PWD/secrets:/secrets:ro" airbyte/source-dropbox:dev \
  check --config /secrets/config.json
```

`config.json` must use one of the authentication shapes in `source_dropbox/spec.json`:

```json
{
  "credentials": {
    "auth_type": "oauth2_pkce",
    "app_key": "your-dropbox-app-key",
    "refresh_token": "your-refresh-token"
  },
  "path": "",
  "recursive": true,
  "include_deleted": true
}
```

## License

MIT
