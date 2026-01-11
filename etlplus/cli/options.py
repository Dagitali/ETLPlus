"""
:mod:`etlplus.cli.options` module.

Shared Typer helper utilities for command-line interface (CLI) option
configuration.
"""

from __future__ import annotations

from .types import DataConnectorContext

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'typer_format_option_kwargs',
]


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
