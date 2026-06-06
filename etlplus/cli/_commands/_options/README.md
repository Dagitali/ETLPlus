# `etlplus.cli._commands._options` Subpackage

Documentation for the `etlplus.cli._commands._options` subpackage: internal Typer option aliases
shared by CLI command modules.

- Centralizes common option declarations to keep help text consistent
- Groups options by command family and resource type
- Supports command callbacks in `etlplus.cli._commands`

Back to CLI command internals: see [`../README.md`](../README.md).

- [Audience](#audience)
- [Module Layout](#module-layout)
- [See Also](#see-also)

## Audience

This is a private CLI implementation package. Users should call the installed `etlplus` command or
import documented Python facades such as `etlplus.ops`, not these option alias modules.

## Module Layout

- `common.py`: shared config, output, verbosity, event, and history options.
- `resources.py`: source and target resource arguments plus format/type overrides.
- `specs.py`: check, render, transform, and validation option aliases.
- `history.py`: history, log, status, and report option aliases.
- `init.py` and `ui.py`: command-specific option aliases.
- `helpers.py`: small factories for Typer option declarations.

## See Also

- CLI command internals in [`../README.md`](../README.md)
- CLI package overview in [`../../README.md`](../../README.md)
