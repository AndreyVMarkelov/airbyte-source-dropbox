# Airbyte Source Dropbox

Airbyte source connector for Dropbox with incremental sync, metadata ingestion, and optional document extraction.

## Status

Early development.

## Planned streams

- `entries` — files, folders, and deletions from Dropbox `list_folder`
- `files` — file metadata snapshot
- `folders` — folder metadata snapshot
- `file_contents` — optional extracted text for supported document formats

## Authentication

The connector is designed to use a Dropbox refresh token with an app key. PKCE authorization tooling will be added before the first release.

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
