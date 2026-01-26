"""
:mod:`etlplus.cli.main` module.

Entry point helpers for the Typer-powered ``etlplus`` CLI.

This module exposes :func:`main` for the console script as well as
:func:`create_parser` for callers that still need an ``argparse`` parser.
"""

from __future__ import annotations

import contextlib
import sys
import warnings

import click
import typer

from .commands import app

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'main',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _emit_context_help(
    ctx: click.Context | None,
) -> bool:
    """
    Mirror Click help output for the provided context onto STDERR.

    Parameters
    ----------
    ctx : click.Context | None
        The Click context to emit help for.

    Returns
    -------
    bool
        ``True`` when help was emitted, ``False`` when *ctx* was ``None``.
    """
    if ctx is None:
        return False

    with contextlib.redirect_stdout(sys.stderr):
        print(ctx.get_help())
    return True


def _emit_root_help(
    command: click.Command,
) -> None:
    """
    Print the root ``etlplus`` help text to stderr.

    Parameters
    ----------
    command : click.Command
        The root Typer/Click command.
    """
    ctx = command.make_context('etlplus', [], resilient_parsing=True)
    try:
        _emit_context_help(ctx)
    finally:
        ctx.close()


def _is_illegal_option_error(
    exc: click.exceptions.UsageError,
) -> bool:
    """
    Return ``True`` when usage errors stem from invalid options.

    Parameters
    ----------
    exc : click.exceptions.UsageError
        The usage error to inspect.

    Returns
    -------
    bool
        ``True`` when the error indicates illegal options.
    """
    return isinstance(
        exc,
        (
            click.exceptions.BadOptionUsage,
            click.exceptions.BadParameter,
            click.exceptions.NoSuchOption,
        ),
    )


def _is_unknown_command_error(
    exc: click.exceptions.UsageError,
) -> bool:
    """
    Return ``True`` when a :class:`UsageError` indicates bad subcommand.

    Parameters
    ----------
    exc : click.exceptions.UsageError
        The usage error to inspect.

    Returns
    -------
    bool
        ``True`` when the error indicates an unknown command.
    """
    message = getattr(exc, 'message', None) or str(exc)
    return message.startswith('No such command ')


# SECTION: FUNCTIONS ======================================================== #


def create_parser() -> object:
    """
    Deprecated legacy entrypoint.

    The argparse-based parser has been removed. Use the Typer-powered
    ``etlplus`` CLI instead (``etlplus.cli.commands.app``).
    """
    warnings.warn(
        'create_parser is deprecated and no longer returns an argparse '
        'parser. Use the Typer CLI entrypoint instead.',
        DeprecationWarning,
        stacklevel=2,
    )
    raise RuntimeError(
        'The legacy argparse parser has been removed. Invoke the Typer-based '
        'CLI via `etlplus` or import `etlplus.cli.commands.app`.',
    )


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
    click.exceptions.UsageError
        Re-raises Typer/Click usage errors after printing help for unknown
        commands.
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

    except click.exceptions.UsageError as exc:
        if _is_unknown_command_error(exc):
            typer.echo(f'Error: {exc}', err=True)
            _emit_root_help(command)
            return int(getattr(exc, 'exit_code', 2))
        if _is_illegal_option_error(exc):
            typer.echo(f'Error: {exc}', err=True)
            if not _emit_context_help(exc.ctx):
                _emit_root_help(command)
            return int(getattr(exc, 'exit_code', 2))

        raise

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
