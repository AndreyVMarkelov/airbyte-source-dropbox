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

Each native file-reference record includes `client_modified` when Dropbox returns
a valid timestamp, normalized to UTC. It also includes `server_modified` as
provenance only; neither timestamp participates in the byte-version cursor.
`server_modified` is Dropbox-owned and is never treated as destination metadata.

The first run traverses the selected root with `files_list_folder`, transfers the
snapshot, and stores Dropbox's opaque list-folder cursor. Later runs use
`files_list_folder_continue` to process only Dropbox deltas. State keeps both the
cursor and a file map keyed by stable Dropbox file ID with revision, content
hash, and pinned destination-relative path. New or changed byte versions transfer
again; unchanged versions are skipped.

The destination forwards each state checkpoint only after every preceding native
file reference or propagation control has committed to Dropbox. If an upload,
move, or delete fails, the durable state keeps the old Dropbox cursor and Airbyte
replays the same delta.

State is also bound to the configured root path and `recursive` setting. A scope
change with rename or deletion propagation enabled fails before any destination
mutation; with both policies set to `ignore`, the connector starts a safe fresh
inventory for the new scope.

Existing version-1 state has no scope binding. Upgrade it with both propagation
policies set to `ignore` once; that run performs a safe fresh inventory and
emits scoped cursor state. Version-2 scoped state without a Dropbox cursor is
also upgraded by one fresh inventory. Rename or deletion propagation intentionally
fails closed for legacy unscoped state until a scoped checkpoint exists. If
Dropbox resets an existing cursor, the connector fails closed instead of
guessing a destructive replay plan.

`rename_policy` and `delete_policy` both default to `ignore`. Set
`rename_policy` to `propagate` to move unchanged files at the destination
without downloading their bytes. Set `delete_policy` to `delete` to remove a
destination file only after Dropbox emits a deletion tombstone for the pinned
source path. Propagated moves fail if the target path already exists; neither
policy overwrites an unrelated destination file.

Files above `file_transfer.max_file_size_mb` are skipped before download. Because
Dropbox cursor state may still advance after the page is processed, increasing
the size limit later does not by itself replay that unchanged oversized file; run
a fresh sync or cause a new Dropbox change event if you need to pick it up.

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
