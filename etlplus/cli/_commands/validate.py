"""
:mod:`etlplus.cli._commands.validate` module.

Typer command for validating data against JSON-described rules.
"""

from __future__ import annotations

import typer

from .. import _handlers as handlers
from .._state import ensure_state
from .app import app
from .helpers import normalize_file_format
from .helpers import normalize_resource_type
from .helpers import parse_json_option
from .helpers import resolve_logged_resource_type
from .options import OutputOption
from .options import RulesOption
from .options import SourceArg
from .options import SourceFormatOption
from .options import SourceTypeOption
from .options import StructuredEventFormatOption

# SECTION: EXPORTS ========================================================== #


__all__ = ['validate_cmd']


# SECTION: FUNCTIONS ======================================================== #


@app.command('validate')
def validate_cmd(
    ctx: typer.Context,
    rules: RulesOption = '{}',
    source: SourceArg = '-',
    source_format: SourceFormatOption = None,
    source_type: SourceTypeOption = None,
    output: OutputOption = '-',
    event_format: StructuredEventFormatOption = None,
) -> int:
    """
    Validate data against JSON-described rules.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    rules : RulesOption, optional
        JSON string describing the validation rules (defaults to '{}').
    source : SourceArg, optional
        Source resource (defaults to '-').
    source_format : SourceFormatOption, optional
        Format of the source resource (defaults to None).
    source_type : SourceTypeOption, optional
        Type of the source resource (defaults to None).
    output : OutputOption, optional
        Path to output file for rendered SQL (defaults to stdout).
    event_format : StructuredEventFormatOption, optional
        Format for structured events (defaults to None).

    Returns
    -------
    int
        Exit code (0 if checks passed, non-zero if any checks failed).
    """
    source_format = normalize_file_format(
        source_format,
        label='source_format',
    )
    source_type = normalize_resource_type(
        source_type,
        label='source_type',
    )
    state = ensure_state(ctx)
    resolve_logged_resource_type(
        state,
        role='source',
        value=source,
        explicit_type=source_type,
        soft_inference=True,
    )

    return int(
        handlers.validate_handler(
            source=source,
            rules=parse_json_option(rules, '--rules'),
            event_format=event_format,
            source_format=source_format,
            target=output,
            format_explicit=source_format is not None,
            pretty=state.pretty,
        ),
    )
