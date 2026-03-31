"""
:mod:`etlplus.cli._handlers` module.

Command handler functions for the ``etlplus`` command-line interface (CLI).
"""

from __future__ import annotations

import sys
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
from time import sleep
from typing import Any
from typing import Literal
from typing import cast

from .. import Config
from .. import __version__
from ..database import render_tables
from ..file import File
from ..history import HistoryStore
from ..history import RunCompletion
from ..history import RunState
from ..history import build_run_record
from ..ops import extract
from ..ops import load
from ..ops import run
from ..ops import transform
from ..ops import validate
from ..runtime import ReadinessReportBuilder
from ..runtime import RuntimeEvents
from ..utils._types import JSONData
from ..utils._types import TemplateKey
from . import _handler_check as _check_impl
from . import _handler_common as _common_impl
from . import _handler_dataops as _dataops_impl
from . import _handler_history as _history_impl
from . import _handler_render as _render_impl
from . import _io
from . import _summary
from ._history import HistoryReportBuilder
from ._history import HistoryView
from ._summary import check_sections as _check_sections
from ._summary import pipeline_summary as _pipeline_summary

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'check_handler',
    'extract_handler',
    'history_handler',
    'load_handler',
    'render_handler',
    'report_handler',
    'run_handler',
    'status_handler',
    'transform_handler',
    'validate_handler',
]


# SECTION: INTERNAL TYPE ALIASES ============================================ #


type _CompletionMode = Literal['file', 'json', 'json_file', 'or_write']


_CommandContext = _common_impl.CommandContext


# SECTION: INTERNAL CONSTANTS =============================================== #


# Keep these module attributes available for tests that monkeypatch the facade.
_PATCHABLE_EXPORTS = (
    File,
    RunCompletion,
    RunState,
    ReadinessReportBuilder,
    RuntimeEvents,
    HistoryReportBuilder,
    HistoryView,
    _io,
    _summary,
)


# SECTION: INTERNAL FUNCTIONS =============================================== #


_complete_command = _common_impl.complete_command
_fail_command = _common_impl.fail_command


@contextmanager
def _failure_boundary(
    context: _CommandContext,
    *,
    on_error: Callable[[Exception], None] | None = None,
    **fields: Any,
) -> Iterator[None]:
    """Emit a failed lifecycle event for exceptions raised inside the block."""
    try:
        yield
    except Exception as exc:
        if on_error is not None:
            on_error(exc)
        _fail_command(context, exc, **fields)
        raise


_load_history_records = _history_impl.load_history_records


def _complete_output(
    context: _CommandContext,
    payload: Any,
    *,
    mode: _CompletionMode,
    pretty: bool = True,
    output_path: str | None = None,
    format_hint: str | None = None,
    success_message: str | None = None,
    **fields: Any,
) -> int:
    """Emit completion for *context* and route the payload by output mode."""
    return _common_impl.complete_output(
        context,
        payload,
        mode=mode,
        complete_command=_complete_command,
        pretty=pretty,
        output_path=output_path,
        format_hint=format_hint,
        success_message=success_message,
        **fields,
    )


def check_handler(
    *,
    config: str | None = None,
    jobs: bool = False,
    pipelines: bool = False,
    readiness: bool = False,
    sources: bool = False,
    summary: bool = False,
    targets: bool = False,
    transforms: bool = False,
    substitute: bool = True,
    pretty: bool = True,
) -> int:
    """
    Print requested pipeline sections from a YAML configuration.

    Parameters
    ----------
    config : str | None, optional
        Path to the YAML configuration file. Default is ``None``.
    jobs : bool, optional
        Whether to include jobs in the output. Default is ``False``.
    pipelines : bool, optional
        Whether to include pipelines in the output. Default is ``False``.
    readiness : bool, optional
        Whether to include readiness checks in the output. Default is ``False``.
    sources : bool, optional
        Whether to include sources in the output. Default is ``False``.
    summary : bool, optional
        Whether to include a summary in the output. Default is ``False``.
    targets : bool, optional
        Whether to include targets in the output. Default is ``False``.
    transforms : bool, optional
        Whether to include transforms in the output. Default is ``False``.
    substitute : bool, optional
        Whether to perform variable substitution. Default is ``True``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.
    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
    """
    return _check_impl.check_handler(
        config=config,
        jobs=jobs,
        pipelines=pipelines,
        readiness=readiness,
        sources=sources,
        summary=summary,
        targets=targets,
        transforms=transforms,
        substitute=substitute,
        pretty=pretty,
        check_sections=_check_sections,
        pipeline_summary=_pipeline_summary,
    )


def history_handler(
    *,
    follow: bool = False,
    job: str | None = None,
    json_output: bool = False,
    limit: int | None = None,
    raw: bool = False,
    pretty: bool = True,
    run_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    status: str | None = None,
    table: bool = False,
) -> int:
    """
    Emit persisted local run history.

    Parameters
    ----------
    follow : bool, optional
        Whether to follow the history in real-time. Default is ``False``.
    job : str | None, optional
        Optional job name to filter history records. Default is ``None``.
    json_output : bool, optional
        Whether to output history in JSON format. Default is ``False``.
    limit : int | None, optional
        Optional limit on the number of history records to retrieve. Default is
        ``None``.
    raw : bool, optional
        Whether to output raw history records. Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.
    run_id : str | None, optional
        Optional run ID to filter history records. Default is ``None``.
    since : str | None, optional
        Optional start time to filter history records. Default is ``None``.
    until : str | None, optional
        Optional end time to filter history records. Default is ``None``.
    status : str | None, optional
        Optional status to filter history records. Default is ``None``.
    table : bool, optional
        Whether to include table information in the output. Default is ``False``.

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
    """
    return _history_impl.history_handler(
        follow=follow,
        job=job,
        json_output=json_output,
        limit=limit,
        raw=raw,
        pretty=pretty,
        run_id=run_id,
        since=since,
        until=until,
        status=status,
        table=table,
        load_records=_load_history_records,
        sleep_fn=sleep,
    )


def extract_handler(
    *,
    source_type: str,
    source: str,
    event_format: str | None = None,
    format_hint: str | None = None,
    format_explicit: bool = False,
    target: str | None = None,
    output: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Extract data from a source.

    Parameters
    ----------
    source_type : str
        The type of the source to extract data from.
    source : str
        The source from which to extract data.
    event_format : str | None, optional
        Optional format of the events. Default is ``None``.
    format_hint : str | None, optional
        Optional hint for the format. Default is ``None``.
    format_explicit : bool, optional
        Whether the format is explicitly specified. Default is ``False``.
    target : str | None, optional
        Optional target to extract data to. Default is ``None``.
    output : str | None, optional
        Optional path to write the extracted output to. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
    """
    return _dataops_impl.extract_handler(
        source_type=source_type,
        source=source,
        event_format=event_format,
        format_hint=format_hint,
        format_explicit=format_explicit,
        target=target,
        output=output,
        pretty=pretty,
        extract_fn=extract,
    )


def load_handler(
    *,
    source: str,
    target_type: str,
    target: str,
    event_format: str | None = None,
    source_format: str | None = None,
    target_format: str | None = None,
    format_explicit: bool = False,
    output: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Load data into a target.

    Parameters
    ----------
    source : str
        The source from which to load data.
    target_type : str
        The type of the target to load data into.
    target : str
        The target into which to load data.
    event_format : str | None, optional
        Optional format of the events. Default is ``None``.
    source_format : str | None, optional
        Optional format of the source data. Default is ``None``.
    target_format : str | None, optional
        Optional format of the target data. Default is ``None``.
    format_explicit : bool, optional
        Whether the format is explicitly specified. Default is ``False``.
    output : str | None, optional
        Optional path to write the loaded output to. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
    """
    return _dataops_impl.load_handler(
        source=source,
        target_type=target_type,
        target=target,
        event_format=event_format,
        source_format=source_format,
        target_format=target_format,
        format_explicit=format_explicit,
        output=output,
        pretty=pretty,
        load_fn=load,
    )


def render_handler(
    *,
    config: str | None = None,
    spec: str | None = None,
    table: str | None = None,
    template: TemplateKey | None = None,
    template_path: str | None = None,
    output: str | None = None,
    pretty: bool = True,
    quiet: bool = False,
) -> int:
    """
    Render SQL DDL statements from table schema specs.

    Parameters
    ----------
    config : str | None, optional
        Optional path to a config file containing table schema specs.
        Default is ``None``.
    spec : str | None, optional
        Optional path to a single table schema spec file. If provided, this
        takes precedence over any specs defined in a config file. Default is
        ``None``.
    table : str | None, optional
        Optional table name for filtering specs. Matches against the 'table' or
        'name' field in specs. If provided, only specs matching this table name
        will be rendered. Default is ``None``.
    template : TemplateKey | None, optional
        Optional key of template to use for rendering. If not provided, a
        default template will be used. Default is ``None``.
    template_path : str | None, optional
        Optional path to a custom template file. If provided, this will
        override the template specified by the *template* parameter. Default is
        ``None``.
    output : str | None, optional
        Optional path to write the rendered output to. If not provided, output
        will be printed to stdout. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print the rendered output. Default is ``True``.
    quiet : bool, optional
        Whether to suppress output. Default is ``False``.

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
    """
    return _render_impl.render_handler(
        config=config,
        spec=spec,
        table=table,
        template=template,
        template_path=template_path,
        output=output,
        pretty=pretty,
        quiet=quiet,
        render_tables_fn=render_tables,
        print_fn=print,
        stderr=sys.stderr,
    )


def report_handler(
    *,
    group_by: Literal['day', 'job', 'status'] = 'job',
    job: str | None = None,
    json_output: bool = False,
    pretty: bool = True,
    since: str | None = None,
    table: bool = False,
    until: str | None = None,
) -> int:
    """Emit a grouped history report derived from normalized persisted runs.

    Parameters
    ----------
    group_by : Literal['day', 'job', 'status'], optional
        The criterion by which to group the history report. Default is 'job'.
    job : str | None, optional
        Optional job name to filter the history report. Default is ``None``.
    json_output : bool, optional
        Whether to output the report in JSON format. Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.
    since : str | None, optional
        Optional start date for filtering the history report. Default is ``None``.
    table : bool, optional
        Whether to include a table in the report. Default is ``False``.
    until : str | None, optional
        Optional end date for filtering the history report. Default is ``None``.

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
    """
    return _history_impl.report_handler(
        group_by=group_by,
        job=job,
        json_output=json_output,
        pretty=pretty,
        since=since,
        table=table,
        until=until,
        load_records=_load_history_records,
    )


def run_handler(
    *,
    config: str,
    job: str | None = None,
    pipeline: str | None = None,
    event_format: str | None = None,
    pretty: bool = True,
) -> int:
    """Execute an ETL job end-to-end from a pipeline YAML configuration.

    Parameters
    ----------
    config : str
        Path to the pipeline YAML configuration file.
    job : str | None, optional
        Optional job name to execute. Default is ``None``.
    pipeline : str | None, optional
        Optional pipeline name to execute. Default is ``None``.
    event_format : str | None, optional
        Optional event format. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
    """
    cfg = Config.from_yaml(config, substitute=True)

    job_name = job or pipeline
    if not job_name:
        return _common_impl.emit_json_payload(_pipeline_summary(cfg), pretty=pretty)

    context = _common_impl.start_command(
        command='run',
        event_format=event_format,
        config_path=config,
        etlplus_version=__version__,
        job=job_name,
        pipeline_name=cfg.name,
        status='running',
    )
    history_store = HistoryStore.from_environment()
    history_store.record_run_started(
        build_run_record(
            run_id=context.run_id,
            config_path=config,
            started_at=context.started_at,
            pipeline_name=cfg.name,
            job_name=job_name,
        ),
    )

    with _failure_boundary(
        context,
        on_error=lambda exc: _common_impl.record_run_completion(
            history_store,
            context,
            status='failed',
            exc=exc,
        ),
        config_path=config,
        job=job_name,
        pipeline_name=cfg.name,
    ):
        result = run(job=job_name, config_path=config)

    _common_impl.record_run_completion(
        history_store,
        context,
        status='succeeded',
        result_summary=cast(JSONData | None, result),
    )
    return _complete_output(
        context,
        {'run_id': context.run_id, 'status': 'ok', 'result': result},
        mode='json',
        pretty=pretty,
        config_path=config,
        job=job_name,
        pipeline_name=cfg.name,
        result_status=result.get('status'),
        status='ok',
    )


def status_handler(
    *,
    job: str | None = None,
    pretty: bool = True,
    run_id: str | None = None,
) -> int:
    """
    Emit the latest normalized run matching the given filters.

    Parameters
    ----------
    job : str | None, optional
        Optional job name to filter the status report. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.
    run_id : str | None, optional
        Optional run ID to filter the status report. Default is ``None``.

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
    """
    return _history_impl.status_handler(
        job=job,
        pretty=pretty,
        run_id=run_id,
        load_records=_load_history_records,
    )


def transform_handler(
    *,
    source: str,
    operations: JSONData | str,
    target: str | None = None,
    target_type: str | None = None,
    event_format: str | None = None,
    source_format: str | None = None,
    target_format: str | None = None,
    pretty: bool = True,
    format_explicit: bool = False,
) -> int:
    """
    Transform data from a source.

    Parameters
    ----------
    source : str
        The source from which to transform data.
    operations : JSONData | str
        The transformation operations to perform, either as a JSON object or a
        path to a JSON file containing the operations.
    target : str | None, optional
        Optional target to transform data to. Default is ``None``.
    target_type : str | None, optional
        Optional type of the target to transform data into. Default is ``None``.
    event_format : str | None, optional
        Optional format of the events. Default is ``None``.
    source_format : str | None, optional
        Optional format of the source data. Default is ``None``.
    target_format : str | None, optional
        Optional format of the target data. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.
    format_explicit : bool, optional
        Whether the formats are explicitly specified. Default is ``False``.

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.

    Notes
    -----
    File targets are written directly. Non-file targets such as ``api`` and
    ``database`` are delegated to :func:`etlplus.ops.load.load` so the
    transform command and load command share target behavior.
    """
    return _dataops_impl.transform_handler(
        source=source,
        operations=operations,
        target=target,
        target_type=target_type,
        event_format=event_format,
        source_format=source_format,
        target_format=target_format,
        pretty=pretty,
        format_explicit=format_explicit,
        load_fn=load,
        transform_fn=transform,
    )


def validate_handler(
    *,
    source: str,
    rules: JSONData | str,
    event_format: str | None = None,
    source_format: str | None = None,
    target: str | None = None,
    format_explicit: bool = False,
    pretty: bool = True,
) -> int:
    """
    Validate data from a source.

    Parameters
    ----------
    source : str
        The source from which to validate data.
    rules : JSONData | str
        The validation rules to apply, either as a JSON object or a path to a
        JSON file containing the rules.
    event_format : str | None, optional
        Optional format of the events. Default is ``None``.
    source_format : str | None, optional
        Optional format of the source data. Default is ``None``.
    target : str | None, optional
        Optional target to validate data against. Default is ``None``.
    format_explicit : bool, optional
        Whether the formats are explicitly specified. Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print the output. Default is ``True``.

    Returns
    -------
    int
        Exit code ``0`` on success; non-zero on error.
    """
    return _dataops_impl.validate_handler(
        source=source,
        rules=rules,
        event_format=event_format,
        source_format=source_format,
        target=target,
        format_explicit=format_explicit,
        pretty=pretty,
        validate_fn=validate,
        print_fn=print,
        stderr=sys.stderr,
    )
