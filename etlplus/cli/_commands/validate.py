"""
:mod:`etlplus.cli._commands.validate` module.

Typer command for validating data against JSON-described rules.
"""

from __future__ import annotations

from typing import cast

import typer

from etlplus.cli import _handlers as handlers
from etlplus.cli._commands.app import app
from etlplus.cli._commands.helpers import _parse_json_option
from etlplus.cli._commands.options import OutputOption
from etlplus.cli._commands.options import RulesOption
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
    source_format = cast(
        SourceFormatOption,
        ResourceTypeResolver.optional_choice(
            source_format,
            FILE_FORMATS,
            label='source_format',
        ),
    )
    source_type = ResourceTypeResolver.optional_choice(
        source_type,
        DATA_CONNECTORS,
        label='source_type',
    )
    state = ensure_state(ctx)
    resolved_source_type = source_type or ResourceTypeResolver.infer_soft(source)

    log_inferred_resource(
        state,
        role='source',
        value=source,
        resource_type=resolved_source_type,
    )

    return int(
        handlers.validate_handler(
            source=source,
            rules=_parse_json_option(rules, '--rules'),
            event_format=event_format,
            source_format=source_format,
            target=output,
            format_explicit=source_format is not None,
            pretty=state.pretty,
        ),
    )
