"""
:mod:`etlplus.cli._commands.extract` module.

Typer command for extracting data from supported sources.
"""

from __future__ import annotations

import typer

from .. import _handlers as handlers
from .._state import ensure_state
from .app import app
from .helpers import call_handler
from .helpers import resolve_resource
from .options import SourceArg
from .options import SourceFormatOption
from .options import SourceTypeOption
from .options import StructuredEventFormatOption

# SECTION: EXPORTS ========================================================== #


__all__ = ['extract_cmd']


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
        Source to extract data from.
    source_format : SourceFormatOption, optional
        Format of the source data.
    source_type : SourceTypeOption, optional
        Type of the source.
    event_format : StructuredEventFormatOption, optional
        Format of structured events.

    Returns
    -------
    int
        Exit code (0 if extraction succeeded, non-zero if any errors occurred).

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
        handlers.extract_handler,
        state=state,
        source_type=resolved_source.resource_type,
        source=resolved_source.value,
        event_format=event_format,
        format_hint=resolved_source.format_hint,
        format_explicit=resolved_source.format_hint is not None,
    )
