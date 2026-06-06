# `etlplus.cli._handlers` Subpackage

Documentation for the `etlplus.cli._handlers` subpackage: private command-handler implementations
behind the `etlplus` CLI.

- Keeps command callbacks thin by moving command behavior into handler modules
- Owns shared payload parsing, lifecycle event emission, output formatting, and completion helpers
- Implements handler functions for data operations, history commands, rendering, scheduling, and
  pipeline execution

Back to CLI overview: see [`etlplus.cli`](../README.md).

- [Audience](#audience)
- [Module Layout](#module-layout)
- [See Also](#see-also)

## Audience

This is a private implementation package. Users should rely on the installed CLI or documented
Python facades such as `etlplus.ops`, `etlplus.history`, and `etlplus.runtime`.

## Module Layout

- `dataops.py`: handlers for `extract`, `load`, `transform`, and `validate`.
- `run.py`: pipeline execution handler.
- `history.py`: handlers for `history`, `log`, `status`, and `report`.
- `check.py`, `render.py`, `schedule.py`, and `init.py`: command-specific handlers.
- Underscore-prefixed helpers: shared input, output, payload, lifecycle, summary, completion, and
  report utilities.

## See Also

- CLI command internals in [`../_commands/README.md`](../_commands/README.md)
- CLI package overview in [`../README.md`](../README.md)
- Runtime package overview in [`../../runtime/README.md`](../../runtime/README.md)
