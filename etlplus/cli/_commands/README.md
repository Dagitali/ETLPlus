# `etlplus.cli._commands` Subpackage

Documentation for the `etlplus.cli._commands` subpackage: internal Typer command registration and
command callback modules for the `etlplus` CLI.

- Owns the root Typer app object and root callback
- Registers command callbacks for `check`, `extract`, `history`, `init`, `load`, `log`, `render`,
  `report`, `run`, `schedule`, `status`, `transform`, `ui`, and `validate`
- Defines shared option aliases and command state helpers

Back to CLI overview: see [`etlplus.cli`](../README.md).

- [Audience](#audience)
- [Module Layout](#module-layout)
- [See Also](#see-also)

## Audience

This is an internal implementation package for the installed `etlplus` command. Users should rely on
the documented CLI commands and `etlplus <command> --help`, not import command callback modules
directly.

## Module Layout

- `_app.py`: shared Typer application object.
- `_constants.py`: CLI description, epilog, and shared command constants.
- `_root.py`: root callback and global flags.
- `_state.py`: shared CLI state and resource-type resolution.
- `_options/`: reusable Typer option aliases.
- Command modules such as `check.py`, `run.py`, and `validate.py`: Typer callbacks for individual
  commands.

## See Also

- CLI package overview in [`../README.md`](../README.md)
- CLI handlers in [`../_handlers/README.md`](../_handlers/README.md)
- Main README command reference in [README.md](../../../README.md)
