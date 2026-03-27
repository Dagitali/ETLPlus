"""
:mod:`etlplus.cli.options` module.

Shared Typer helper utilities for command-line interface (CLI) option
configuration.
"""

from __future__ import annotations

from typing import Literal

from ._types import DataConnectorContext

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'typer_format_option_kwargs',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _typer_connector_option_kwargs(
    *,
    context: DataConnectorContext,
    rich_help_panel: str = 'I/O overrides',
) -> dict[str, object]:
    """Return common Typer option kwargs for source/target connector types."""
    return {
        'metavar': 'CONNECTOR',
        'show_default': False,
        'rich_help_panel': rich_help_panel,
        'help': f'Override the inferred {context} type (api, database, file).',
    }


def _typer_flag_option_kwargs(
    help_text: str,
    *,
    is_eager: bool = False,
    show_default: bool | None = None,
) -> dict[str, object]:
    """Return common Typer option kwargs for simple boolean flags."""
    kwargs: dict[str, object] = {'help': help_text}
    if is_eager:
        kwargs['is_eager'] = True
    if show_default is not None:
        kwargs['show_default'] = show_default
    return kwargs


def _typer_resource_argument_kwargs(
    *,
    context: DataConnectorContext,
) -> dict[str, object]:
    """Return common Typer argument kwargs for source/target resources."""
    if context == 'source':
        description = 'JSON payload, file path, URI/URL, or - for STDIN'
        verb = 'Extract data from'
    else:
        description = 'file path, URI/URL, or - for STDOUT'
        verb = 'Load data into'
    return {
        'metavar': context.upper(),
        'help': (
            f'{verb} {context.upper()} ({description}). Use '
            f'--{context}-format to override '
            'the inferred data format and '
            f'--{context}-type to override the inferred data connector.'
        ),
    }


def _typer_path_option_kwargs(
    help_text: str,
    *,
    metavar: str = 'PATH',
    show_default: bool | None = False,
) -> dict[str, object]:
    """Return common Typer option kwargs for path-like string inputs."""
    kwargs: dict[str, object] = {
        'help': help_text,
        'metavar': metavar,
    }
    if show_default is not None:
        kwargs['show_default'] = show_default
    return kwargs


def _typer_timestamp_option_kwargs(
    *,
    bound: Literal['since', 'until'],
) -> dict[str, object]:
    """Return common Typer option kwargs for ISO-8601 history bounds."""
    direction = 'after' if bound == 'since' else 'before'
    return {
        'metavar': 'ISO8601',
        'show_default': False,
        'help': (f'Emit only records at or {direction} the given ISO-8601 timestamp.'),
    }


# SECTION: FUNCTIONS ======================================================== #


def typer_format_option_kwargs(
    *,
    context: DataConnectorContext,
    rich_help_panel: str = 'Format overrides',
) -> dict[str, object]:
    """
    Return common Typer option kwargs for format overrides.

    Parameters
    ----------
    context : DataConnectorContext
        Either ``'source'`` or ``'target'`` to tailor help text.
    rich_help_panel : str, optional
        The rich help panel name. Default is ``'Format overrides'``.

    Returns
    -------
    dict[str, object]
        The Typer option keyword arguments.
    """
    return {
        'metavar': 'FORMAT',
        'show_default': False,
        'rich_help_panel': rich_help_panel,
        'help': (
            f'Payload format when the {context} is STDIN/inline or a '
            'non-file connector. File connectors infer from extensions.'
        ),
    }
