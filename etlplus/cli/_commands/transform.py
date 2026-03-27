"""
:mod:`etlplus.cli._commands.transform` module.

Typer command for JSON-described record transformations.
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

    source_format = normalize_file_format(
        source_format,
        label='source_format',
    )
    source_type = normalize_resource_type(
        source_type,
        label='source_type',
    )
    target_format = normalize_file_format(
        target_format,
        label='target_format',
    )
    target_type = normalize_resource_type(
        target_type,
        label='target_type',
    )

    resolved_source_value = source if source is not None else '-'
    resolved_target_value = target if target is not None else '-'
    resolve_logged_resource_type(
        state,
        role='source',
        value=resolved_source_value,
        explicit_type=source_type,
        soft_inference=True,
    )
    resolved_target_type = resolve_logged_resource_type(
        state,
        role='target',
        value=resolved_target_value,
        explicit_type=target_type,
    )
    assert resolved_target_type is not None

    return int(
        handlers.transform_handler(
            source=resolved_source_value,
            operations=parse_json_option(operations, '--operations'),
            target=resolved_target_value,
            target_type=resolved_target_type,
            event_format=event_format,
            source_format=source_format,
            target_format=target_format,
            format_explicit=target_format is not None,
            pretty=state.pretty,
        ),
    )
