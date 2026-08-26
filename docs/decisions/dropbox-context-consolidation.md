# Dropbox context consolidation decision

## Status

Deferred.

## Context

Dropbox authentication, Business team context, and Path Root handling currently exist in
four package-local modules:

- `source_dropbox/dropbox_context.py`
- `connectors/source-dropbox-files/source_dropbox_files/dropbox_context.py`
- `connectors/destination-dropbox-files/destination_dropbox_files/dropbox_context.py`
- `connectors/dropbox-repair/dropbox_repair/dropbox_context.py`

The duplicated behavior is protected by characterization tests in
`tests/context_contract/test_dropbox_context_contract.py`. Those tests lock down
the observable contract for:

- personal access-token auth;
- OAuth refresh-token auth;
- Select-User and Select-Admin routing;
- Path Root `default`, `home`, `root`, and `namespace_id`;
- state-safe effective namespace binding for `home` and `root`;
- equivalent SDK routing across the root source, native file-transfer source,
  native file-transfer destination, and repair tool.

## Decision

Do not consolidate these modules into a shared implementation yet.

The clean consolidation target would be a small internal package such as
`airbyte_dropbox_common`. In the current repository layout, however, the
standalone connectors are intentionally independent packages with their own
`pyproject.toml`, lockfile, Docker image, and CI job.

The nested connector Dockerfiles currently install only their connector package:

- `pip install --no-cache-dir ./connectors/source-dropbox-files`
- `pip install --no-cache-dir ./connectors/destination-dropbox-files`

A shared package would require at least one of:

- path dependencies from nested connector packages to a monorepo-local package;
- Dockerfile changes to copy and install that package alongside each connector;
- lockfile changes for every nested package;
- packaging configuration that reaches outside the connector package root.

That adds build/package coupling for a small amount of stable code and makes the
standalone connector story harder to reason about. It also risks introducing
different behavior between local `uv run --project ... --frozen`, Docker builds,
and root-package tests.

## Consequences

The duplication remains, but it is intentional and contract-tested.

Future changes to Dropbox context behavior must update all package-local copies
and keep the cross-package characterization tests green.

Reconsider consolidation only if the repository adopts one of these simpler
packaging models:

- a real monorepo workspace where every connector can depend on an internal
  package without Docker/source-path special cases;
- a separately published internal package;
- a single distribution boundary for all Dropbox connectors.

Until then, preserving simple standalone connector builds is more valuable than
removing this duplication.
