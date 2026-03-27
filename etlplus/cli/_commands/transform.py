"""
:mod:`etlplus.cli._commands.transform` module.

Typer command for JSON-described record transformations.
"""

from __future__ import annotations

import typer

from .. import _handlers as handlers
from ._state import ensure_state
from .app import app
from .helpers import call_handler
from .helpers import parse_json_option
from .helpers import resolve_resource
from .options import OperationsOption
from .options import SourceArg
from .options import SourceFormatOption
from .options import SourceTypeOption
from .options import StructuredEventFormatOption
from .options import TargetArg
from .options import TargetFormatOption
from .options import TargetTypeOption

# SECTION: EXPORTS ========================================================== #


__all__ = ['transform_cmd']


# SECTION: FUNCTIONS ======================================================== #


@app.command('transform')
def transform_cmd(
    ctx: typer.Context,
    operations: OperationsOption = '{}',
    source: SourceArg = '-',
    source_format: SourceFormatOption = None,
    source_type: SourceTypeOption = None,
    target: TargetArg = '-',
    target_format: TargetFormatOption = None,
    target_type: TargetTypeOption = None,
    event_format: StructuredEventFormatOption = None,
) -> int:
    """
    Transform records using JSON-described operations.

    Parameters
    ----------
    ctx : typer.Context
        Typer context.
    operations : OperationsOption, optional
        JSON string describing the transformation operations (defaults to '{}').
    source : SourceArg, optional
        Source resource (defaults to '-').
    source_format : SourceFormatOption, optional
        Format of the source resource (defaults to None).
    source_type : SourceTypeOption, optional
        Type of the source resource (defaults to None).
    target : TargetArg, optional
        Target resource (defaults to '-').
    target_format : TargetFormatOption, optional
        Format of the target resource (defaults to None).
    target_type : TargetTypeOption, optional
        Type of the target resource (defaults to None).
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
    resolved_target = resolve_resource(
        state,
        role='target',
        value=target,
        connector_type=target_type,
        format_value=target_format,
    )
    assert resolved_target.resource_type is not None

    return call_handler(
        handlers.transform_handler,
        state=state,
        source=resolved_source.value,
        operations=parse_json_option(operations, '--operations'),
        target=resolved_target.value,
        target_type=resolved_target.resource_type,
        event_format=event_format,
        source_format=resolved_source.format_hint,
        target_format=resolved_target.format_hint,
        format_explicit=resolved_target.format_hint is not None,
    )
