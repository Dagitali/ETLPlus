"""
:mod:`etlplus.cli._commands.extract` module.

Typer command for extracting data from supported sources.
"""

from __future__ import annotations

from typing import cast

import typer

from etlplus.cli import _handlers as handlers
from etlplus.cli._commands.app import app
from etlplus.cli._commands.helpers import normalize_choice
from etlplus.cli._commands.helpers import require_positional_argument
from etlplus.cli._commands.options import SourceArg
from etlplus.cli._commands.options import SourceFormatOption
from etlplus.cli._commands.options import SourceTypeOption
from etlplus.cli._commands.options import StructuredEventFormatOption
from etlplus.cli._constants import DATA_CONNECTORS
from etlplus.cli._constants import FILE_FORMATS
from etlplus.cli._state import ResourceTypeResolver
from etlplus.cli._state import ensure_state
from etlplus.cli._state import log_inferred_resource

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
    source_type = normalize_choice(
        source_type,
        DATA_CONNECTORS,
        label='source_type',
    )
    source_format = cast(
        SourceFormatOption,
        normalize_choice(
            source_format,
            FILE_FORMATS,
            label='source_format',
        ),
    )

    resolved_source_type = source_type or ResourceTypeResolver.infer_or_exit(source)

    log_inferred_resource(
        state,
        role='source',
        value=source,
        resource_type=resolved_source_type,
    )

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
