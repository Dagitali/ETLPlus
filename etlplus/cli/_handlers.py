"""
:mod:`etlplus.cli.handlers` module.

Command handler functions for the ``etlplus`` command-line interface (CLI).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from pathlib import Path
from time import perf_counter
from typing import Any
from typing import Literal
from typing import cast

from .. import Config
from .. import __version__
from ..database import load_table_spec
from ..database import render_tables
from ..file import File
from ..file import FileFormat
from ..history import build_run_record
from ..history import iter_history_runs
from ..history import open_history_store
from ..ops import extract
from ..ops import load
from ..ops import run
from ..ops import transform
from ..ops import validate
from ..ops.validate import FieldRulesDict
from ..runtime import build_readiness_report
from ..runtime.events import build_structured_event
from ..runtime.events import create_run_id
from ..runtime.events import emit_structured_event
from ..runtime.events import utc_now_iso
from ..utils.types import JSONData
from ..utils.types import TemplateKey
from . import _io

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'check_handler',
    'extract_handler',
    'history_handler',
    'load_handler',
    'render_handler',
    'run_handler',
    'transform_handler',
    'validate_handler',
]


# SECTION: TYPE ALIASES ===================================================== #


TransformOperations = Mapping[
    Literal['filter', 'map', 'select', 'sort', 'aggregate'],
    Any,
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _collect_table_specs(
    config_path: str | None,
    spec_path: str | None,
) -> list[dict[str, Any]]:
    """
    Load table schemas from a pipeline config and/or standalone spec.

    Parameters
    ----------
    config_path : str | None
        Path to a pipeline YAML config file.
    spec_path : str | None
        Path to a standalone table spec file.

    Returns
    -------
    list[dict[str, Any]]
        Collected table specification mappings.
    """
    specs: list[dict[str, Any]] = []

    if spec_path:
        specs.append(dict(load_table_spec(Path(spec_path))))

    if config_path:
        cfg = Config.from_yaml(config_path, substitute=True)
        specs.extend(getattr(cfg, 'table_schemas', []))

    return specs


def _check_sections(
    cfg: Config,
    *,
    jobs: bool,
    pipelines: bool,
    sources: bool,
    targets: bool,
    transforms: bool,
) -> dict[str, Any]:
    """
    Build sectioned metadata output for the check command.

    Parameters
    ----------
    cfg : Config
        The loaded pipeline configuration.
    jobs : bool
        Whether to include job metadata.
    pipelines : bool
        Whether to include pipeline metadata.
    sources : bool
        Whether to include source metadata.
    targets : bool
        Whether to include target metadata.
    transforms : bool
        Whether to include transform metadata.

    Returns
    -------
    dict[str, Any]
        Metadata output for the check command.
    """
    sections: dict[str, Any] = {}
    if jobs:
        sections['jobs'] = _pipeline_summary(cfg)['jobs']
    if pipelines:
        sections['pipelines'] = [cfg.name]
    if sources:
        sections['sources'] = [src.name for src in cfg.sources]
    if targets:
        sections['targets'] = [tgt.name for tgt in cfg.targets]
    if transforms:
        if isinstance(cfg.transforms, Mapping):
            sections['transforms'] = list(cfg.transforms)
        else:
            sections['transforms'] = [
                getattr(trf, 'name', None) for trf in cfg.transforms
            ]
    if not sections:
        sections['jobs'] = _pipeline_summary(cfg)['jobs']
    return sections


def _emit_failure_event(
    *,
    command: str,
    run_id: str,
    started_perf: float,
    event_format: str | None,
    exc: Exception,
    **fields: Any,
) -> None:
    """Emit a failure event with the shared stable schema."""
    _emit_lifecycle_event(
        command=command,
        lifecycle='failed',
        run_id=run_id,
        event_format=event_format,
        duration_ms=int((perf_counter() - started_perf) * 1000),
        error_message=str(exc),
        error_type=type(exc).__name__,
        status='error',
        **fields,
    )


def _emit_lifecycle_event(
    *,
    command: str,
    lifecycle: str,
    run_id: str,
    event_format: str | None,
    **fields: Any,
) -> None:
    """Emit one structured command lifecycle event."""
    emit_structured_event(
        build_structured_event(
            command=command,
            lifecycle=lifecycle,
            run_id=run_id,
            **fields,
        ),
        event_format=event_format,
    )


def _history_sort_key(
    record: Mapping[str, Any],
) -> tuple[str, str]:
    """Return a reverse-sortable key for history records."""
    timestamp = cast(
        str,
        record.get('started_at') or record.get('finished_at') or '',
    )
    run_id = cast(str, record.get('run_id') or '')
    return (timestamp, run_id)


def _pipeline_summary(
    cfg: Config,
) -> dict[str, Any]:
    """
    Return a human-friendly snapshot of a pipeline config.

    Parameters
    ----------
    cfg : Config
        The loaded pipeline configuration.

    Returns
    -------
    dict[str, Any]
        A human-friendly snapshot of a pipeline config.
    """
    sources = [src.name for src in cfg.sources]
    targets = [tgt.name for tgt in cfg.targets]
    jobs = [job.name for job in cfg.jobs]
    return {
        'name': cfg.name,
        'version': cfg.version,
        'sources': sources,
        'targets': targets,
        'jobs': jobs,
    }


def _write_file_payload(
    payload: JSONData,
    target: str,
    *,
    format_hint: str | None,
) -> None:
    """
    Write a JSON-like payload to a file path using an optional format hint.

    Parameters
    ----------
    payload : JSONData
        The structured data to write.
    target : str
        File path to write to.
    format_hint : str | None
        Optional format hint for :class:`FileFormat`.
    """
    file_format = FileFormat.coerce(format_hint) if format_hint else None
    File(target, file_format=file_format).write(payload)


# SECTION: FUNCTIONS ======================================================== #


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
        Path to the pipeline YAML configuration.
    jobs : bool, optional
        Whether to include job metadata. Default is ``False``.
    pipelines : bool, optional
        Whether to include pipeline metadata. Default is ``False``.
    readiness : bool, optional
        Whether to run runtime and config readiness checks. Default is
        ``False``.
    sources : bool, optional
        Whether to include source metadata. Default is ``False``.
    summary : bool, optional
        Whether to print a full summary of the pipeline. Default is ``False``.
    targets : bool, optional
        Whether to include target metadata. Default is ``False``.
    transforms : bool, optional
        Whether to include transform metadata. Default is ``False``.
    substitute : bool, optional
        Whether to perform environment variable substitution. Default is
        ``True``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.

    Raises
    ------
    ValueError
        If config inspection is requested without a configuration path.

    """
    if readiness:
        report = build_readiness_report(config_path=config)
        _io.emit_json(report, pretty=pretty)
        return 0 if report.get('status') == 'ok' else 1

    if config is None:
        raise ValueError('config is required unless readiness-only mode is used')

    cfg = Config.from_yaml(config, substitute=substitute)
    if summary:
        _io.emit_json(_pipeline_summary(cfg), pretty=True)
        return 0

    _io.emit_json(
        _check_sections(
            cfg,
            jobs=jobs,
            pipelines=pipelines,
            sources=sources,
            targets=targets,
            transforms=transforms,
        ),
        pretty=pretty,
    )
    return 0


def history_handler(
    *,
    limit: int | None = None,
    raw: bool = False,
    pretty: bool = True,
) -> int:
    """
    Emit persisted local run history.

    Parameters
    ----------
    limit : int | None, optional
        Maximum number of records to emit. Default is ``None``.
    raw : bool, optional
        Whether to emit raw append events instead of normalized runs.
        Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.
    """
    history_store = open_history_store()
    records_iter = (
        history_store.iter_records()
        if raw
        else iter_history_runs(history_store)
    )
    records = sorted(records_iter, key=_history_sort_key, reverse=True)
    if limit is not None:
        records = records[:limit]
    _io.emit_json(records, pretty=pretty)
    return 0


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
        The type of the source (e.g., 'file', 'api', 'database').
    source : str
        The source identifier (e.g., path, URL, DSN).
    event_format : str | None, optional
        Optional structured event format emitted to STDERR. Default is
        ``None``.
    format_hint : str | None, optional
        An optional format hint (e.g., 'json', 'csv'). Default is ``None``.
    format_explicit : bool, optional
        Whether the format hint was explicitly provided. Default is ``False``.
    target : str | None, optional
        The target destination (e.g., path, database). Default is ``None``.
    output : str | None, optional
        Path to write output data. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.

    Raises
    ------
    Exception
        Re-raises extraction failures after emitting a structured failure event
        when requested.

    """
    explicit_format = format_hint if format_explicit else None
    run_id = create_run_id()
    started_perf = perf_counter()
    _emit_lifecycle_event(
        command='extract',
        lifecycle='started',
        run_id=run_id,
        event_format=event_format,
        source=source,
        source_type=source_type,
    )

    try:
        if source == '-':
            text = _io.read_stdin_text()
            payload = _io.parse_text_payload(
                text,
                format_hint,
            )
            _emit_lifecycle_event(
                command='extract',
                lifecycle='completed',
                run_id=run_id,
                event_format=event_format,
                duration_ms=int((perf_counter() - started_perf) * 1000),
                result_status='ok',
                status='ok',
                source=source,
                source_type=source_type,
            )
            _io.emit_json(payload, pretty=pretty)

            return 0

        result = extract(
            source_type,
            source,
            file_format=explicit_format,
        )
        output_path = target or output

        _emit_lifecycle_event(
            command='extract',
            lifecycle='completed',
            run_id=run_id,
            event_format=event_format,
            destination=output_path or 'stdout',
            duration_ms=int((perf_counter() - started_perf) * 1000),
            result_status='ok',
            source=source,
            source_type=source_type,
            status='ok',
        )

        _io.emit_or_write(
            result,
            output_path,
            pretty=pretty,
            success_message='Data extracted and saved to',
        )
    except Exception as exc:
        _emit_failure_event(
            command='extract',
            run_id=run_id,
            started_perf=started_perf,
            event_format=event_format,
            exc=exc,
            source=source,
            source_type=source_type,
        )
        raise

    return 0


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
        The source payload (e.g., path, inline data).
    target_type : str
        The type of the target (e.g., 'file', 'database').
    target : str
        The target destination (e.g., path, DSN).
    event_format : str | None, optional
        Optional structured event format emitted to STDERR. Default is
        ``None``.
    source_format : str | None, optional
        An optional source format hint (e.g., 'json', 'csv'). Default is
        ``None``.
    target_format : str | None, optional
        An optional target format hint (e.g., 'json', 'csv'). Default is
        ``None``.
    format_explicit : bool, optional
        Whether the format hint was explicitly provided. Default is ``False``.
    output : str | None, optional
        Path to write output data. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.

    Raises
    ------
    Exception
        Re-raises load failures after emitting a structured failure event when
        requested.
    """
    explicit_format = target_format if format_explicit else None
    run_id = create_run_id()
    started_perf = perf_counter()
    _emit_lifecycle_event(
        command='load',
        lifecycle='started',
        run_id=run_id,
        event_format=event_format,
        source=source,
        target=target,
        target_type=target_type,
    )

    try:
        # Allow piping into load.
        source_value = cast(
            str | Path | os.PathLike[str] | dict[str, Any] | list[dict[str, Any]],
            _io.resolve_cli_payload(
                source,
                format_hint=source_format,
                format_explicit=source_format is not None,
                hydrate_files=False,
            ),
        )

        # Allow piping out of load for file targets.
        if target_type == 'file' and target == '-':
            payload = _io.materialize_file_payload(
                source_value,
                format_hint=source_format,
                format_explicit=source_format is not None,
            )
            _emit_lifecycle_event(
                command='load',
                lifecycle='completed',
                run_id=run_id,
                event_format=event_format,
                duration_ms=int((perf_counter() - started_perf) * 1000),
                result_status='ok',
                source=source,
                status='ok',
                target=target,
                target_type=target_type,
            )
            _io.emit_json(payload, pretty=pretty)
            return 0

        result = load(
            source_value,
            target_type,
            target,
            file_format=explicit_format,
        )

        output_path = output
        _emit_lifecycle_event(
            command='load',
            lifecycle='completed',
            run_id=run_id,
            event_format=event_format,
            destination=output_path or 'stdout',
            duration_ms=int((perf_counter() - started_perf) * 1000),
            result_status=result.get('status') if isinstance(result, dict) else 'ok',
            source=source,
            status='ok',
            target=target,
            target_type=target_type,
        )
        _io.emit_or_write(
            result,
            output_path,
            pretty=pretty,
            success_message='Load result saved to',
        )
    except Exception as exc:
        _emit_failure_event(
            command='load',
            run_id=run_id,
            started_perf=started_perf,
            event_format=event_format,
            exc=exc,
            source=source,
            target=target,
            target_type=target_type,
        )
        raise

    return 0


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
        Path to a pipeline YAML configuration. Default is ``None``.
    spec : str | None, optional
        Path to a standalone table spec file. Default is ``None``.
    table : str | None, optional
        Table name filter. Default is ``None``.
    template : TemplateKey | None, optional
        The template key to use for rendering. Default is ``None``.
    template_path : str | None, optional
        Path to a custom template file. Default is ``None``.
    output : str | None, optional
        Path to write output SQL. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.
    quiet : bool, optional
        Whether to suppress non-error output. Default is ``False``.

    Returns
    -------
    int
        Zero on success.
    """
    template_value: TemplateKey = template or 'ddl'
    template_path_override = template_path
    table_filter = table
    spec_path = spec
    config_path = config

    # If the provided template points to a file, treat it as a path override.
    file_override = template_path_override
    template_key: TemplateKey | None = template_value
    if template_path_override is None:
        candidate_path = Path(template_value)
        if candidate_path.exists():
            file_override = str(candidate_path)
            template_key = None

    specs = _collect_table_specs(config_path, spec_path)
    if table_filter:
        specs = [
            spec
            for spec in specs
            if str(spec.get('table')) == table_filter
            or str(spec.get('name', '')) == table_filter
        ]

    if not specs:
        target_desc = table_filter or 'table_schemas'
        print(
            'No table schemas found for '
            f'{target_desc}. Provide --spec or a pipeline --config with '
            'table_schemas.',
            file=sys.stderr,
        )
        return 1

    rendered_chunks = render_tables(
        specs,
        template=template_key,
        template_path=file_override,
    )
    sql_text = '\n'.join(chunk.rstrip() for chunk in rendered_chunks).rstrip() + '\n'
    rendered_output = sql_text if pretty else sql_text.rstrip('\n')

    output_path = output
    if output_path and output_path != '-':
        Path(output_path).write_text(rendered_output, encoding='utf-8')
        if not quiet:
            print(f'Rendered {len(specs)} schema(s) to {output_path}')
        return 0

    print(rendered_output)
    return 0


def run_handler(
    *,
    config: str,
    job: str | None = None,
    pipeline: str | None = None,
    event_format: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Execute an ETL job end-to-end from a pipeline YAML configuration.

    Parameters
    ----------
    config : str
        Path to the pipeline YAML configuration.
    job : str | None, optional
        Name of the job to run. If not provided, runs the entire pipeline.
        Default is ``None``.
    pipeline : str | None, optional
        Alias for *job*. Default is ``None``.
    event_format : str | None, optional
        Optional structured event format emitted to STDERR. Default is
        ``None``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.

    Raises
    ------
    Exception
        Re-raises the underlying execution error after emitting a failure
        event when structured event output is enabled.
    """
    cfg = Config.from_yaml(config, substitute=True)

    job_name = job or pipeline
    if job_name:
        run_id = create_run_id()
        started_at = utc_now_iso()
        started_perf = perf_counter()
        history_store = open_history_store()
        history_store.record_run_started(
            build_run_record(
                run_id=run_id,
                config_path=config,
                started_at=started_at,
                pipeline_name=cfg.name,
                job_name=job_name,
            ),
        )
        _emit_lifecycle_event(
            command='run',
            lifecycle='started',
            run_id=run_id,
            event_format=event_format,
            config_path=config,
            etlplus_version=__version__,
            job=job_name,
            pipeline_name=cfg.name,
            status='running',
            timestamp=started_at,
        )
        try:
            result = run(job=job_name, config_path=config)
        except Exception as exc:
            duration_ms = int((perf_counter() - started_perf) * 1000)
            history_store.record_run_finished(
                run_id,
                status='failed',
                finished_at=utc_now_iso(),
                duration_ms=duration_ms,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            _emit_failure_event(
                command='run',
                run_id=run_id,
                started_perf=started_perf,
                event_format=event_format,
                exc=exc,
                config_path=config,
                job=job_name,
                pipeline_name=cfg.name,
            )
            raise

        duration_ms = int((perf_counter() - started_perf) * 1000)
        history_store.record_run_finished(
            run_id,
            status='succeeded',
            finished_at=utc_now_iso(),
            duration_ms=duration_ms,
            result_summary=cast(JSONData | None, result),
        )
        _emit_lifecycle_event(
            command='run',
            lifecycle='completed',
            run_id=run_id,
            event_format=event_format,
            config_path=config,
            duration_ms=duration_ms,
            job=job_name,
            pipeline_name=cfg.name,
            result_status=result.get('status'),
            status='ok',
        )
        _io.emit_json(
            {'run_id': run_id, 'status': 'ok', 'result': result},
            pretty=pretty,
        )
        return 0

    _io.emit_json(_pipeline_summary(cfg), pretty=pretty)
    return 0


def transform_handler(
    *,
    source: str,
    operations: JSONData | str,
    target: str | None = None,
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
        The source payload (e.g., path, inline data).
    operations : JSONData | str
        The transformation operations (inline JSON or path).
    target : str | None, optional
        The target destination (e.g., path). Default is ``None``.
    event_format : str | None, optional
        Optional structured event format emitted to STDERR. Default is
        ``None``.
    source_format : str | None, optional
        An optional source format hint (e.g., 'json', 'csv'). Default is
        ``None``.
    target_format : str | None, optional
        An optional target format hint (e.g., 'json', 'csv'). Default is
        ``None``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.
    format_explicit : bool, optional
        Whether the format hint was explicitly provided. Default is ``False``.

    Returns
    -------
    int
        Zero on success.

    Raises
    ------
    ValueError
        If the operations payload is not a mapping.
    Exception
        Re-raises transform failures after emitting a structured failure event
        when requested.
    """
    format_hint: str | None = source_format
    format_explicit = format_hint is not None or format_explicit
    run_id = create_run_id()
    started_perf = perf_counter()
    _emit_lifecycle_event(
        command='transform',
        lifecycle='started',
        run_id=run_id,
        event_format=event_format,
        source=source,
        target=target or 'stdout',
    )

    try:
        payload = cast(
            JSONData | str,
            _io.resolve_cli_payload(
                source,
                format_hint=format_hint,
                format_explicit=format_explicit,
            ),
        )

        operations_payload = _io.resolve_cli_payload(
            operations,
            format_hint=None,
            format_explicit=format_explicit,
        )
        if not isinstance(operations_payload, dict):
            raise ValueError('operations must resolve to a mapping of transforms')

        data = transform(payload, cast(TransformOperations, operations_payload))

        # TODO: Generalize to handle non-file targets.
        if target and target != '-':
            _emit_lifecycle_event(
                command='transform',
                lifecycle='completed',
                run_id=run_id,
                event_format=event_format,
                duration_ms=int((perf_counter() - started_perf) * 1000),
                result_status='ok',
                source=source,
                status='ok',
                target=target,
            )
            _write_file_payload(data, target, format_hint=target_format)
            print(f'Data transformed and saved to {target}')
            return 0

        _emit_lifecycle_event(
            command='transform',
            lifecycle='completed',
            run_id=run_id,
            event_format=event_format,
            duration_ms=int((perf_counter() - started_perf) * 1000),
            result_status='ok',
            source=source,
            status='ok',
            target=target or 'stdout',
        )
        _io.emit_json(data, pretty=pretty)
    except Exception as exc:
        _emit_failure_event(
            command='transform',
            run_id=run_id,
            started_perf=started_perf,
            event_format=event_format,
            exc=exc,
            source=source,
            target=target or 'stdout',
        )
        raise

    return 0


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
        The source payload (e.g., path, inline data).
    rules : JSONData | str
        The validation rules (inline JSON or path).
    event_format : str | None, optional
        Optional structured event format emitted to STDERR. Default is
        ``None``.
    source_format : str | None, optional
        An optional source format hint (e.g., 'json', 'csv'). Default is
        ``None``.
    target : str | None, optional
        The target destination (e.g., path). Default is ``None``.
    format_explicit : bool, optional
        Whether the format hint was explicitly provided. Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.

    Raises
    ------
    ValueError
        If the rules payload is not a mapping.
    Exception
        Re-raises validation failures after emitting a structured failure event
        when requested.
    """
    format_hint: str | None = source_format
    run_id = create_run_id()
    started_perf = perf_counter()
    _emit_lifecycle_event(
        command='validate',
        lifecycle='started',
        run_id=run_id,
        event_format=event_format,
        source=source,
        target=target or 'stdout',
    )

    try:
        payload = cast(
            JSONData | str,
            _io.resolve_cli_payload(
                source,
                format_hint=format_hint,
                format_explicit=format_explicit,
            ),
        )

        rules_payload = _io.resolve_cli_payload(
            rules,
            format_hint=None,
            format_explicit=format_explicit,
        )
        if not isinstance(rules_payload, dict):
            raise ValueError('rules must resolve to a mapping of field rules')

        field_rules = cast(Mapping[str, FieldRulesDict], rules_payload)
        result = validate(payload, field_rules)

        if target and target != '-':
            validated_data = result.get('data')
            if validated_data is not None:
                _emit_lifecycle_event(
                    command='validate',
                    lifecycle='completed',
                    run_id=run_id,
                    event_format=event_format,
                    duration_ms=int((perf_counter() - started_perf) * 1000),
                    result_status='ok',
                    source=source,
                    status='ok',
                    target=target,
                    valid=result.get('valid'),
                )
                _io.write_json_output(
                    validated_data,
                    target,
                    success_message='ValidationDict result saved to',
                )
            else:
                print(
                    f'ValidationDict failed, no data to save for {target}',
                    file=sys.stderr,
                )
        else:
            _emit_lifecycle_event(
                command='validate',
                lifecycle='completed',
                run_id=run_id,
                event_format=event_format,
                duration_ms=int((perf_counter() - started_perf) * 1000),
                result_status='ok',
                source=source,
                status='ok',
                target=target or 'stdout',
                valid=result.get('valid'),
            )
            _io.emit_json(result, pretty=pretty)
    except Exception as exc:
        _emit_failure_event(
            command='validate',
            run_id=run_id,
            started_perf=started_perf,
            event_format=event_format,
            exc=exc,
            source=source,
            target=target or 'stdout',
        )
        raise

    return 0
