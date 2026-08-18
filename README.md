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
- `file_contents` — opt-in full-refresh Markdown extraction for configured document extensions.

## Authentication

### Recommended: PKCE refresh token

1. Create a scoped Dropbox app in the [Dropbox App Console](https://www.dropbox.com/developers/apps). Choose **Full Dropbox** access if the connector must read existing account content.
2. In the app's **Permissions** tab, enable the scopes for the streams you plan to use:
   - Core streams (`entries`, `files`, `folders`): `account_info.read`, `files.metadata.read`.
   - `shared_links`, `shared_folders`: `sharing.read`.
   - `file_contents`: `files.content.read`.
3. Run the helper locally (it uses PKCE and never asks for a client secret):

   ```bash
   python -m source_dropbox.oauth authorize --app-key <APP_KEY>
   ```

   It prints an authorization URL. Approve it in Dropbox, copy the displayed code back into the helper, then paste the generated credentials JSON into the Airbyte UI. The default preset requests every connector scope. To request less, use `--scope-preset core` or `--scope-preset core+sharing`.

The connector deliberately keeps optional scopes local: connection testing verifies only base credentials. A missing `sharing.read` or `files.content.read` scope produces a clear error only if the corresponding stream is selected.

### Development/manual testing: access token

For short-lived local testing, generate an access token in the Dropbox App Console and choose **Access token (development/manual testing only)** in the Airbyte UI. Do not use this mode for production connections; use the PKCE refresh-token flow instead.

`shared_links` and `shared_folders` require the Dropbox `sharing.read` scope. They are
full-refresh streams for governance, migration, and RAG ACL enrichment. The core connection
check and file/folder streams do not require this scope.

`file_contents` requires `files.content.read`. It uses Dropbox Riviera to convert the configured
connector-supported document extensions to Markdown. Select the stream and configure an explicit
`file_contents.allowed_extensions` allow-list; an empty allow-list never extracts content. Files
larger than `file_contents.max_file_size_mb` are skipped. Riviera supports a maximum source size
of 50 MB. OCR and embedded images are intentionally disabled in this version.

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
