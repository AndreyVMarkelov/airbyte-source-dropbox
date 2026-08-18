# Changelog

All notable changes to this connector are documented here.

## Unreleased

- Normalize Dropbox authentication and refresh-token errors at every API boundary.
- Preserve stream-local `sharing.read` and `files.content.read` permission errors.
- Document supported runtime and release compatibility policy.

## 0.1.0

- Initial Dropbox source with incremental `entries`, metadata snapshot, sharing, and optional Riviera Markdown extraction streams.
