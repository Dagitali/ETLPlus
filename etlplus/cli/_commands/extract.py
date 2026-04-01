"""
:mod:`etlplus.cli._commands.extract` module.

Typer command for extracting data from supported sources.
"""

from __future__ import annotations

import typer

from .._handlers.dataops import extract_handler
from ._app import app
from ._helpers import call_handler
from ._helpers import resolve_resource
from ._options.common import StructuredEventFormatOption
from ._options.resources import SourceArg
from ._options.resources import SourceFormatOption
from ._options.resources import SourceTypeOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'extract_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('extract')
def extract_cmd(
    ctx: typer.Context,
    source: SourceArg = '-',
    source_format: SourceFormatOption = None,
    source_type: SourceTypeOption = None,
    event_format: StructuredEventFormatOption = None,
) -> int:
    """
    Extract data from files, databases, or REST APIs.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    source : SourceArg, optional
        Source path, URI/URL, JSON payload, or ``-`` for STDIN.
    source_format : SourceFormatOption, optional
        Source payload format override.
    source_type : SourceTypeOption, optional
        Source connector type override.
    event_format : StructuredEventFormatOption, optional
        Structured event output format.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    state = ensure_state(ctx)
    resolved_source = resolve_resource(
        state,
        role='source',
        value=source,
        connector_type=source_type,
        format_value=source_format,
        positional=True,
    )
    assert resolved_source.resource_type is not None

    return call_handler(
        extract_handler,
        state=state,
        source=resolved_source.value,
        source_type=resolved_source.resource_type,
        source_format=resolved_source.format_hint,
        event_format=event_format,
        format_explicit=resolved_source.format_hint is not None,
    )
