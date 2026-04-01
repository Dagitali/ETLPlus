"""
:mod:`etlplus.cli._commands.validate` module.

Typer command for validating data against JSON-described rules.
"""

from __future__ import annotations

import typer

from .._handlers.dataops import validate_handler
from ._app import app
from ._helpers import call_handler
from ._helpers import parse_json_option
from ._helpers import resolve_resource
from ._options.common import OutputOption
from ._options.common import StructuredEventFormatOption
from ._options.resources import SourceArg
from ._options.resources import SourceFormatOption
from ._options.resources import SourceTypeOption
from ._options.specs import RulesOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'validate_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('validate')
def validate_cmd(
    ctx: typer.Context,
    source: SourceArg = '-',
    source_format: SourceFormatOption = None,
    source_type: SourceTypeOption = None,
    rules: RulesOption = '{}',
    output: OutputOption = '-',
    event_format: StructuredEventFormatOption = None,
) -> int:
    """
    Validate data against JSON-described rules.

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
    rules : RulesOption, optional
        JSON string describing the validation rules.
    output : OutputOption, optional
        Optional output path for validation results.
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
        soft_inference=True,
    )

    return call_handler(
        validate_handler,
        state=state,
        source=resolved_source.value,
        rules=parse_json_option(rules, '--rules'),
        target=output,
        source_format=resolved_source.format_hint,
        event_format=event_format,
        format_explicit=resolved_source.format_hint is not None,
    )
