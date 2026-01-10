# Documentation Notes

## CLI Parser Status
- The CLI is now Typer/Click-only. The historical `argparse` parser and `create_parser` entrypoint
  are deprecated and no longer supported for new integrations.
- Downstream tools should invoke the Typer app exported at `etlplus.cli.commands.app` (e.g., `python
  -m etlplus` or `etlplus ...`).
- Handler functions still accept keyword arguments; the legacy namespace shim is temporary and will
  be removed in a future release. Avoid constructing `argparse.Namespace` objects and instead call
  handlers with explicit keyword arguments if you integrate programmatically.

## Migration Hints
- Replace any imports of `etlplus.cli.main.create_parser` with Typer invocations (`etlplus` binary
  or `app` directly).
- If you maintained custom subcommands around the old parser, port them to Typer by attaching to
  `app` or wrapping the `etlplus` executable.
- Tests and examples now target the Typer surface; expect argparse-focused helpers (e.g., namespace
  format flags) to be absent.
