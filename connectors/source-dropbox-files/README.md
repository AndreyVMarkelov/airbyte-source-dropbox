# Source Dropbox Files

`source-dropbox-files` is a dedicated native Airbyte File Transfer source for
original Dropbox bytes. It requires Airbyte platform **1.7+** and CDK 7.25.1.
It is deliberately separate from `source-dropbox`: use the latter for metadata,
changes, sharing, and Riviera Markdown extraction.

The connector transfers every live Dropbox file below `path` (respecting
`recursive`) through Airbyte-managed staging and native file references. It
downloads by Dropbox file ID in bounded chunks, not base64 records. Files above
`file_transfer.max_file_size_mb` are skipped; a file that disappears or changes
during its download is also skipped. Authentication, scope, staging, and
transient-service failures stop the sync.

After the initial run, state tracks each successfully transferred Dropbox file
by stable ID, revision, content hash, and relative path. Unchanged byte versions
are skipped; changed or newly discovered files transfer again. Path-only renames
are intentionally skipped in this version, so move/rename propagation remains a
future capability. The destination forwards each state checkpoint only after
the preceding native file reference has committed to Dropbox.

Dropbox scopes:

- `account_info.read` for connection checks;
- `files.metadata.read` to traverse the selected root; and
- `files.content.read` to transfer original bytes.

Start with [secrets/config.example.json](secrets/config.example.json), then run:

```bash
uv sync --locked
uv run source-dropbox-files spec
```

For the opt-in read-only live-test profile, set
`DROPBOX_FILES_INTEGRATION_CONFIG` to a local config file that selects a folder
with a small and a multi-chunk fixture. The profile must never be supplied with
credentials committed to the repository.

Staged file references are managed by Airbyte and are meaningful only to a
file-reference-aware destination in the same Airbyte execution. Destination
streaming consumption is intentionally deferred to the next milestone.
