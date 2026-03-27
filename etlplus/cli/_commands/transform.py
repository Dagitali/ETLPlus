"""
:mod:`etlplus.cli._commands.transform` module.

Typer command for JSON-described record transformations.
"""

from __future__ import annotations

from typing import cast

import typer

from etlplus.cli import _handlers as handlers
from etlplus.cli._commands.app import app
from etlplus.cli._commands.helpers import parse_json_option
from etlplus.cli._commands.options import OperationsOption
from etlplus.cli._commands.options import SourceArg
from etlplus.cli._commands.options import SourceFormatOption
from etlplus.cli._commands.options import SourceTypeOption
from etlplus.cli._commands.options import StructuredEventFormatOption
from etlplus.cli._commands.options import TargetArg
from etlplus.cli._commands.options import TargetFormatOption
from etlplus.cli._commands.options import TargetTypeOption
from etlplus.cli._constants import DATA_CONNECTORS
from etlplus.cli._constants import FILE_FORMATS
from etlplus.cli._state import ensure_state
from etlplus.cli._state import infer_resource_type_soft
from etlplus.cli._state import log_inferred_resource
from etlplus.cli._state import optional_choice
from etlplus.cli._state import resolve_resource_type
from etlplus.cli._state import validate_choice

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

    source_format = cast(
        SourceFormatOption,
        optional_choice(
            source_format,
            FILE_FORMATS,
            label='source_format',
        ),
    )
    source_type = optional_choice(
        source_type,
        DATA_CONNECTORS,
        label='source_type',
    )
    target_format = cast(
        TargetFormatOption,
        optional_choice(
            target_format,
            FILE_FORMATS,
            label='target_format',
        ),
    )
    target_type = optional_choice(
        target_type,
        DATA_CONNECTORS,
        label='target_type',
    )

    resolved_source_type = source_type or infer_resource_type_soft(source)
    resolved_source_value = source if source is not None else '-'
    resolved_target_value = target if target is not None else '-'

    if resolved_source_type is not None:
        resolved_source_type = validate_choice(
            resolved_source_type,
            DATA_CONNECTORS,
            label='source_type',
        )

    resolved_target_type = resolve_resource_type(
        explicit_type=None,
        override_type=target_type,
        value=resolved_target_value,
        label='target_type',
    )

    log_inferred_resource(
        state,
        role='source',
        value=resolved_source_value,
        resource_type=resolved_source_type,
    )
    log_inferred_resource(
        state,
        role='target',
        value=resolved_target_value,
        resource_type=resolved_target_type,
    )

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
