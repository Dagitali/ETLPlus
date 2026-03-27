"""
:mod:`etlplus.cli._commands.extract` module.

Typer command for extracting data from supported sources.
"""

from __future__ import annotations

import typer

from .. import _handlers as handlers
from .._state import ensure_state
from .app import app
from .helpers import normalize_file_format
from .helpers import normalize_resource_type
from .helpers import require_positional_argument
from .helpers import resolve_logged_resource_type
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

    source = require_positional_argument(source, name='SOURCE')
    source_type = normalize_resource_type(
        source_type,
        label='source_type',
    )
    source_format = normalize_file_format(
        source_format,
        label='source_format',
    )
    resolved_source_type = resolve_logged_resource_type(
        state,
        role='source',
        value=source,
        explicit_type=source_type,
    )
    assert resolved_source_type is not None

    return int(
        handlers.extract_handler(
            source_type=resolved_source_type,
            source=source,
            event_format=event_format,
            format_hint=source_format,
            format_explicit=source_format is not None,
            pretty=state.pretty,
        ),
    )
