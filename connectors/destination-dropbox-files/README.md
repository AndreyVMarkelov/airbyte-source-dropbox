# Destination Dropbox Files

Native Airbyte File Transfer destination for Dropbox, requiring Airbyte platform 1.7+.
It accepts native `AirbyteRecordMessageFileReference` records from a compatible source,
streams the staged local file into a Dropbox upload session, and never loads the full
file into memory. It also accepts internal move/delete control records emitted by
`source-dropbox-files` when that source enables propagation policies. The existing
`destination-dropbox` remains responsible for regular JSON/base64 records.

Required Dropbox scopes: `account_info.read`, `files.metadata.read`, and
`files.content.write`. The write scope is also required for propagated moves and deletes.

`metadata_policy` defaults to `preserve`. When a compatible source supplies a
valid `client_modified` field, this destination normalizes it to UTC and applies
it to the upload-session commit. Sources without Dropbox metadata remain
compatible. Set `metadata_policy` to `ignore` to let Dropbox assign its default
timestamp. `server_modified`, source IDs, revisions, and Dropbox content hashes
are never submitted as destination write metadata.
