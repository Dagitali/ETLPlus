"""
:mod:`etlplus.cli._commands._root` module.

Root callback and global CLI flags for the Typer app.
"""

from __future__ import annotations

import os

import typer

from ... import __version__
from ...runtime import configure_logging
from ...runtime import configure_telemetry
from ._app import app
from ._options.common import PrettyOption
from ._options.common import QuietOption
from ._options.common import VerboseOption
from ._options.common import VersionOption
from ._state import _set_state

# SECTION: INTERNAL FUNCTIONS =============================================== #


@app.callback(invoke_without_command=True)
def _root(
    ctx: typer.Context,
    version: VersionOption = False,
    pretty: PrettyOption = True,
    quiet: QuietOption = False,
    verbose: VerboseOption = False,
) -> None:
    """
    Seed the Typer context with runtime flags and handle root-only options.

    Parameters
    ----------
    ctx : typer.Context
        The Typer command context.
    version : VersionOption, optional
        Show the version and exit. Default is ``False``.
    pretty : PrettyOption, optional
        Whether to pretty-print JSON output. Default is ``True``.
    quiet : QuietOption, optional
        Whether to suppress warnings and non-essential output. Default is
        ``False``.
    verbose : VerboseOption, optional
        Whether to emit extra diagnostics to STDERR. Default is ``False``.

    Raises
    ------
    typer.Exit
        When ``--version`` is provided or no subcommand is invoked.
    """
    state = _set_state(
        ctx,
        pretty=pretty,
        quiet=quiet,
        verbose=verbose,
    )
    configure_logging(
        quiet=state.quiet,
        verbose=state.verbose,
        force=True,
    )
    configure_telemetry(env=os.environ, force=True)

    if version:
        typer.echo(f'etlplus {__version__}')
        raise typer.Exit(0)

    if ctx.invoked_subcommand is None and not ctx.resilient_parsing:
        typer.echo(ctx.command.get_help(ctx))
        raise typer.Exit(0)
