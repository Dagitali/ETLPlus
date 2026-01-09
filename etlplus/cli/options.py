"""
:mod:`etlplus.cli.options` module.

Shared command-line interface (CLI) option helpers for both Typer and argparse
entry points.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from .constants import DEFAULT_FILE_FORMAT
from .constants import FILE_FORMATS
from .types import FormatContext

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'FormatAction',
    # Functions
    'add_argparse_format_options',
    'typer_format_option_kwargs',
]


# SECTION: CLASSES ========================================================== #


class FormatAction(argparse.Action):
    """Record when a format override flag is provided."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[object] | None,
        option_string: str | None = None,
    ) -> None:  # pragma: no cover - argparse wiring
        setattr(namespace, self.dest, values)
        namespace._format_explicit = True


# SECTION: FUNCTIONS ======================================================== #


def add_argparse_format_options(
    parser: argparse.ArgumentParser,
    *,
    context: FormatContext,
) -> None:
    """
    Attach ``--source-format`` and ``--target-format`` arguments.

    Parameters
    ----------
    parser : argparse.ArgumentParser
        Parser receiving the options.
    context : FormatContext
        Either ``'source'`` or ``'target'`` to tailor help text.
    """
    parser.set_defaults(_format_explicit=False)
    parser.add_argument(
        '--source-format',
        choices=sorted(FILE_FORMATS),
        default=DEFAULT_FILE_FORMAT,
        action=FormatAction,
        help=(
            f'Format of the {context}. Overrides filename-based inference '
            'when provided.'
        ),
    )
    parser.add_argument(
        '--target-format',
        choices=sorted(FILE_FORMATS),
        default=DEFAULT_FILE_FORMAT,
        action=FormatAction,
        help=(
            f'Format of the {context}. Overrides filename-based inference '
            'when provided.'
        ),
    )


def typer_format_option_kwargs(
    *,
    context: FormatContext,
    rich_help_panel: str = 'Format overrides',
) -> dict[str, object]:
    """Return common Typer option kwargs for format overrides."""
    return {
        'metavar': 'FORMAT',
        'show_default': False,
        'rich_help_panel': rich_help_panel,
        'help': (
            f'Payload format when the {context} is stdin/inline or a '
            'non-file connector. File connectors infer from extensions.'
        ),
    }
