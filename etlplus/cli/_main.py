"""
:mod:`etlplus.cli._main` module.

Typer-powered console entry point for the ``etlplus`` CLI.

This module exposes :func:`main` for the console script.
"""

from __future__ import annotations

import contextlib
import importlib
import sys
from types import ModuleType
from typing import Any
from typing import cast

import click
import typer

from ._commands import app

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'main',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _load_typer_click_exceptions() -> ModuleType:
    """
    Return Typer's vendored Click exception module when available.

    Returns
    -------
    ModuleType
        Typer's vendored Click exception module on Python/Typer combinations
        that expose it, otherwise the public Click exception module.
    """
    with contextlib.suppress(ModuleNotFoundError):
        return importlib.import_module('typer._click.exceptions')
    return click.exceptions


_TYPER_CLICK_EXCEPTIONS = _load_typer_click_exceptions()

_USAGE_ERROR_TYPES: tuple[type[BaseException], ...] = (
    click.exceptions.UsageError,
    _TYPER_CLICK_EXCEPTIONS.UsageError,
)

_ILLEGAL_OPTION_ERROR_TYPES: tuple[type[BaseException], ...] = (
    click.exceptions.BadOptionUsage,
    click.exceptions.BadParameter,
    click.exceptions.NoSuchOption,
    _TYPER_CLICK_EXCEPTIONS.BadOptionUsage,
    _TYPER_CLICK_EXCEPTIONS.BadParameter,
    _TYPER_CLICK_EXCEPTIONS.NoSuchOption,
)


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
    Print the root ``etlplus`` help text to STDERR.

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
    exc: BaseException,
) -> bool:
    """
    Return ``True`` when usage errors stem from invalid options.

    Parameters
    ----------
    exc : BaseException
        The usage error to inspect.

    Returns
    -------
    bool
        ``True`` when the error indicates illegal options.
    """
    return isinstance(exc, _ILLEGAL_OPTION_ERROR_TYPES)


def _is_unknown_command_error(
    exc: BaseException,
) -> bool:
    """
    Return ``True`` when a :class:`UsageError` indicates bad subcommand.

    Parameters
    ----------
    exc : BaseException
        The usage error to inspect.

    Returns
    -------
    bool
        ``True`` when the error indicates an unknown command.
    """
    message = getattr(exc, 'message', None) or str(exc)
    return message.startswith('No such command ')


# SECTION: FUNCTIONS ======================================================== #


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
    This wrapper keeps Typer/Click parsing behavior while normalizing help
    output and conventional CLI exit codes.
    """
    resolved_argv = sys.argv[1:] if argv is None else list(argv)
    command = cast(click.Command, typer.main.get_command(app))

    try:
        result = command.main(
            args=resolved_argv,
            prog_name='etlplus',
            standalone_mode=False,
        )
        return int(result or 0)

    except _USAGE_ERROR_TYPES as exc:
        if _is_unknown_command_error(exc):
            typer.echo(f'Error: {exc}', err=True)
            _emit_root_help(command)
            return int(getattr(exc, 'exit_code', 2))
        if _is_illegal_option_error(exc):
            typer.echo(f'Error: {exc}', err=True)
            if not _emit_context_help(cast(click.Context | None, cast(Any, exc).ctx)):
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

    except SystemExit:
        raise

    except (OSError, TypeError, ValueError) as exc:
        typer.echo(f'Error: {exc}', err=True)
        return 1
