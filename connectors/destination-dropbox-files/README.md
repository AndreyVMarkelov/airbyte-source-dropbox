# Destination Dropbox Files

Native Airbyte File Transfer destination for Dropbox, requiring Airbyte platform 1.7+.
It accepts only `AirbyteRecordMessageFileReference` records from a compatible source,
streams the staged local file into a Dropbox upload session, and never loads the full
file into memory. The existing `destination-dropbox` remains responsible for regular
JSON/base64 records.

Required Dropbox scopes: `account_info.read`, `files.metadata.read`, and
`files.content.write`.
