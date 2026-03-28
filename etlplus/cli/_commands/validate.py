"""
:mod:`etlplus.cli._commands.validate` module.

Typer command for validating data against JSON-described rules.
"""

from __future__ import annotations

import typer

from .. import _handlers as handlers
from ._helpers import call_handler
from ._helpers import parse_json_option
from ._helpers import resolve_resource
from ._options import OutputOption
from ._options import RulesOption
from ._options import SourceArg
from ._options import SourceFormatOption
from ._options import SourceTypeOption
from ._options import StructuredEventFormatOption
from ._state import ensure_state
from .app import app

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'validate_cmd',
]


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
    state = ensure_state(ctx)
    resolved_source = resolve_resource(
        state,
        role='source',
        value=source,
        connector_type=source_type,
        format_value=source_format,
        soft_inference=True,
    )

    return call_handler(
        handlers.validate_handler,
        state=state,
        source=resolved_source.value,
        rules=parse_json_option(rules, '--rules'),
        event_format=event_format,
        source_format=resolved_source.format_hint,
        target=output,
        format_explicit=resolved_source.format_hint is not None,
    )
