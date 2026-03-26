"""
:mod:`etlplus.cli._commands.root` module.

Root callback and global CLI flags for the Typer app.
"""

from __future__ import annotations

import typer

from etlplus import __version__
from etlplus.cli._commands.app import app
from etlplus.cli._state import CliState
from etlplus.runtime import configure_logging

# SECTION: EXPORTS ========================================================== #


__all__ = ['_root']


# SECTION: INTERNAL FUNCTIONS =============================================== #


@app.callback(invoke_without_command=True)
def _root(
    ctx: typer.Context,
    version: bool = typer.Option(
        False,
        '--version',
        '-V',
        is_eager=True,
        help='Show the version and exit.',
    ),
    pretty: bool = typer.Option(
        True,
        '--pretty/--no-pretty',
        help='Pretty-print JSON output (default: pretty).',
    ),
    quiet: bool = typer.Option(
        False,
        '--quiet',
        '-q',
        help='Suppress warnings and non-essential output.',
    ),
    verbose: bool = typer.Option(
        False,
        '--verbose',
        '-v',
        help='Emit extra diagnostics to STDERR.',
    ),
) -> None:
    """
    Seed the Typer context with runtime flags and handle root-only options.

    Parameters
    ----------
    ctx : typer.Context
        The Typer command context.
    version : bool, optional
        Show the version and exit. Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print JSON output. Default is ``True``.
    quiet : bool, optional
        Whether to suppress warnings and non-essential output. Default is
        ``False``.
    verbose : bool, optional
        Whether to emit extra diagnostics to STDERR. Default is ``False``.

    Raises
    ------
    typer.Exit
        When ``--version`` is provided or no subcommand is invoked.
    """
    ctx.obj = CliState(pretty=pretty, quiet=quiet, verbose=verbose)
    configure_logging(quiet=quiet, verbose=verbose, force=True)

    if version:
        typer.echo(f'etlplus {__version__}')
        raise typer.Exit(0)

    if ctx.invoked_subcommand is None and not ctx.resilient_parsing:
        typer.echo(ctx.command.get_help(ctx))
        raise typer.Exit(0)
