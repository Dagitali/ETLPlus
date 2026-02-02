# Documentation Notes

- [Documentation Notes](#documentation-notes)
  - [CLI Parser Status](#cli-parser-status)
  - [Migration Hints](#migration-hints)

## CLI Parser Status

- The CLI is Typer/Click-only. The historical `argparse` parser has been removed.
- Downstream tools should invoke the Typer app exported at `etlplus.cli.commands.app` (e.g., `python
  -m etlplus` or `etlplus ...`).
- Handler functions still accept keyword arguments; the legacy namespace shim is temporary and will
  be removed in a future release. Avoid constructing `argparse.Namespace` objects and instead call
  handlers with explicit keyword arguments if you integrate programmatically.

## Migration Hints

- Replace any `argparse`-based integrations with Typer invocations (`etlplus` binary or `app`
  directly).
- If you maintained custom subcommands around the old parser, port them to Typer by attaching to
  `app` or wrapping the `etlplus` executable.
- Tests and examples now target the Typer surface; expect argparse-focused helpers (e.g., namespace
  format flags) to be absent.
