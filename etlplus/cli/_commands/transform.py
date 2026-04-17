"""
:mod:`etlplus.cli._commands.transform` module.

Typer command for JSON-described record transformations.
"""

from __future__ import annotations

import typer

from .._handlers.dataops import transform_handler
from ._app import app
from ._helpers import call_handler
from ._helpers import parse_json_option
from ._helpers import resolve_command_resource
from ._options.common import StructuredEventFormatOption
from ._options.resources import SourceArg
from ._options.resources import SourceFormatOption
from ._options.resources import SourceTypeOption
from ._options.resources import TargetArg
from ._options.resources import TargetFormatOption
from ._options.resources import TargetTypeOption
from ._options.specs import OperationsOption
from ._state import ensure_state

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'transform_cmd',
]


# SECTION: FUNCTIONS ======================================================== #


@app.command('transform')
def transform_cmd(
    ctx: typer.Context,
    source: SourceArg = '-',
    source_format: SourceFormatOption = None,
    source_type: SourceTypeOption = None,
    target: TargetArg = '-',
    target_format: TargetFormatOption = None,
    target_type: TargetTypeOption = None,
    operations: OperationsOption = '{}',
    event_format: StructuredEventFormatOption = None,
) -> int:
    """
    Transform records using JSON-described operations.

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
    target : TargetArg, optional
        Target path, URI/URL, or ``-`` for standard output.
    target_format : TargetFormatOption, optional
        Target payload format override.
    target_type : TargetTypeOption, optional
        Target connector type override.
    operations : OperationsOption, optional
        JSON string describing the transformation operations.
    event_format : StructuredEventFormatOption, optional
        Structured event output format.

    Returns
    -------
    int
        CLI exit code indicating success (``0``) or failure (non-zero).
    """
    state = ensure_state(ctx)
    _, resolved_source = resolve_command_resource(
        ctx,
        state=state,
        role='source',
        value=source,
        connector_type=source_type,
        format_value=source_format,
        soft_inference=True,
    )
    _, resolved_target = resolve_command_resource(
        ctx,
        state=state,
        role='target',
        value=target,
        connector_type=target_type,
        format_value=target_format,
    )

    return call_handler(
        transform_handler,
        state=state,
        source=resolved_source.value,
        source_format=resolved_source.format_hint,
        target=resolved_target.value,
        operations=parse_json_option(operations, '--operations'),
        target_format=resolved_target.format_hint,
        target_type=resolved_target.require_resource_type(),
        event_format=event_format,
        format_explicit=resolved_target.format_explicit,
    )
