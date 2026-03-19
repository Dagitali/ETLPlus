# Documentation Notes

- [Documentation Notes](#documentation-notes)
  - [CLI Parser Status](#cli-parser-status)
  - [Migration Hints](#migration-hints)

## CLI Parser Status

- The CLI is Typer/Click-only. The historical `argparse` parser has been removed.
- The supported invocation surface is the installed `etlplus` command or `python -m etlplus`.
- If you are integrating ETLPlus into your own Python code, prefer the documented public APIs in
  `etlplus.api`, `etlplus.ops`, and the published file/storage helpers instead of importing CLI
  implementation modules.

## Migration Hints

- Replace any `argparse`-based integrations with the supported command surface (`etlplus ...` or
  `python -m etlplus ...`).
- If you maintained custom wrappers around the old parser, migrate them to shell out to the
  installed CLI or move the workflow into the documented Python APIs.
- Tests and examples now target the Typer surface; expect argparse-focused helpers (e.g., namespace
  format flags) to be absent.
