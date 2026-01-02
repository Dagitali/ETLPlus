"""
:mod:`etlplus.cli.main` module.

Entry point helpers for the Typer-powered ``etlplus`` CLI.

This module exposes :func:`main` for the console script as well as
:func:`create_parser` for callers that still need an ``argparse`` parser.
"""

from __future__ import annotations

import argparse
import sys

import typer

from .. import __version__
from .. import cli_compat_argparse
from .app import PROJECT_URL
from .app import app
from .handlers import FORMAT_ENV_KEY
from .handlers import cmd_extract
from .handlers import cmd_list
from .handlers import cmd_load
from .handlers import cmd_pipeline
from .handlers import cmd_run
from .handlers import cmd_transform
from .handlers import cmd_validate

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'create_parser',
    'main',
]


# SECTION: FUNCTIONS ======================================================== #


# -- Parser -- #


# TODO: Sunset this function.
def create_parser() -> argparse.ArgumentParser:
    """
    Return the legacy :mod:`argparse` parser wired to current handlers.

    Returns
    -------
    argparse.ArgumentParser
        Parser compatible with historical ``etlplus`` entry points.
    """

    return cli_compat_argparse.create_parser(
        project_url=PROJECT_URL,
        format_env_key=FORMAT_ENV_KEY,
        version=__version__,
        cmd_extract=cmd_extract,
        cmd_validate=cmd_validate,
        cmd_transform=cmd_transform,
        cmd_load=cmd_load,
        cmd_pipeline=cmd_pipeline,
        cmd_list=cmd_list,
        cmd_run=cmd_run,
    )


# -- Main -- #


def main(
    argv: list[str] | None = None,
) -> int:
    """
    Run the Typer-powered CLI and normalize exit codes.

    Parameters
    ----------
    argv : list[str] | None, optional
        Sequence of command-line arguments excluding the program name. When
        ``None``, defaults to ``sys.argv[1:]``.

    Returns
    -------
    int
        A conventional POSIX exit code: zero on success, non-zero on error.

    Raises
    ------
    SystemExit
        Re-raises SystemExit exceptions to preserve exit codes.

    Notes
    -----
    This function uses Typer (Click) for parsing/dispatch, but preserves the
    existing `cmd_*` handlers by adapting parsed arguments into an
    :class:`argparse.Namespace`.
    """
    resolved_argv = sys.argv[1:] if argv is None else list(argv)
    command = typer.main.get_command(app)

    try:
        result = command.main(
            args=resolved_argv,
            prog_name='etlplus',
            standalone_mode=False,
        )
        return int(result or 0)

    except typer.Exit as exc:
        return int(exc.exit_code)

    except typer.Abort:
        return 1

    except KeyboardInterrupt:  # pragma: no cover - interactive path
        # Conventional exit code for SIGINT
        return 130

    except SystemExit as e:
        print(f'Error: {e}', file=sys.stderr)
        raise e

    except (OSError, TypeError, ValueError) as e:
        print(f'Error: {e}', file=sys.stderr)
        return 1
