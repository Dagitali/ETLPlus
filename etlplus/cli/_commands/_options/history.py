"""
:mod:`etlplus.cli._commands._options.history` module.

History and report Typer option aliases for CLI command modules.
"""

from __future__ import annotations

from typing import Literal

from .helpers import typer_flag_option_alias
from .helpers import typer_timestamp_option_alias
from .helpers import typer_value_option_alias

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Types
    'HistoryFollowOption',
    'HistoryJsonOption',
    'HistoryLevelOption',
    'HistoryLimitOption',
    'HistoryPipelineOption',
    'HistoryRawOption',
    'HistorySinceOption',
    'HistoryStatusOption',
    'HistoryTableOption',
    'HistoryUntilOption',
    'ReportGroupByOption',
    'RunIdOption',
]


# SECTION: TYPES ============================================================ #


HistoryFollowOption = typer_flag_option_alias(
    '--follow',
    help_text='Keep polling for newly persisted matching raw history events.',
)

HistoryJsonOption = typer_flag_option_alias(
    '--json',
    help_text='Format output as JSON explicitly.',
)

HistoryLevelOption = typer_value_option_alias(
    Literal['run', 'job'],
    '--level',
    help_text='Query run-level or job-level persisted history.',
    show_default=True,
)

HistoryLimitOption = typer_value_option_alias(
    int | None,
    '--limit',
    help_text='Maximum number of history records to emit.',
    min=1,
)

HistoryPipelineOption = typer_value_option_alias(
    str | None,
    '--pipeline',
    help_text='Filter persisted history by pipeline name.',
)

HistoryRawOption = typer_flag_option_alias(
    '--raw',
    help_text='Emit raw append events instead of normalized runs.',
)

HistorySinceOption = typer_timestamp_option_alias(
    str | None,
    '--since',
    bound='since',
)

HistoryStatusOption = typer_value_option_alias(
    str | None,
    '--status',
    help_text='Filter persisted runs by status.',
)

HistoryTableOption = typer_flag_option_alias(
    '--table',
    help_text='Format normalized history output as a Markdown table.',
)

HistoryUntilOption = typer_timestamp_option_alias(
    str | None,
    '--until',
    bound='until',
)

ReportGroupByOption = typer_value_option_alias(
    Literal['day', 'job', 'pipeline', 'run', 'status'],
    '--group-by',
    help_text='Grouping dimension for aggregated history reports.',
    show_default=True,
)

RunIdOption = typer_value_option_alias(
    str | None,
    '--run-id',
    help_text='Filter persisted runs by run identifier.',
)
