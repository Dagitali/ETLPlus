"""
:mod:`etlplus.cli.handlers` module.

Command handler functions for the ``etlplus`` command-line interface (CLI).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import Literal
from typing import cast

from ..config import PipelineConfig
from ..config import load_pipeline_config
from ..database import load_table_spec
from ..database import render_tables
from ..extract import extract
from ..file import File
from ..load import load
from ..run import run
from ..transform import transform
from ..types import JSONData
from ..types import TemplateKey
from ..validate import FieldRules
from ..validate import validate
from . import io as cli_io

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'extract_handler',
    'check_handler',
    'load_handler',
    'render_handler',
    'run_handler',
    'transform_handler',
    'validate_handler',
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
        cfg = load_pipeline_config(config_path, substitute=True)
        specs.extend(getattr(cfg, 'table_schemas', []))

    return specs


def _check_sections(
    cfg: PipelineConfig,
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
    cfg : PipelineConfig
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
        sections['transforms'] = [
            getattr(trf, 'name', None) for trf in cfg.transforms
        ]
    if not sections:
        sections['jobs'] = _pipeline_summary(cfg)['jobs']
    return sections


def _pipeline_summary(
    cfg: PipelineConfig,
) -> dict[str, Any]:
    """
    Return a human-friendly snapshot of a pipeline config.

    Parameters
    ----------
    cfg : PipelineConfig
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


# SECTION: FUNCTIONS ======================================================== #


def check_handler(
    args: object | None = None,
    *,
    config: str | None = None,
    jobs: bool = False,
    pipelines: bool = False,
    sources: bool = False,
    summary: bool = False,
    targets: bool = False,
    transforms: bool = False,
    pretty: bool = True,
) -> int:
    """
    Print requested pipeline sections from a YAML configuration.

    Parameters
    ----------
    args : object | None, optional
        Parsed command-line arguments. If provided, other parameters are
        ignored. Default is ``None``.
    config : str | None, optional
        Path to the pipeline YAML configuration. Required if ``args`` is
        ``None``. Default is ``None``.
    jobs : bool, optional
        Whether to include job metadata. Default is ``False``.
    pipelines : bool, optional
        Whether to include pipeline metadata. Default is ``False``.
    sources : bool, optional
        Whether to include source metadata. Default is ``False``.
    summary : bool, optional
        Whether to print a full summary of the pipeline. Default is ``False``.
    targets : bool, optional
        Whether to include target metadata. Default is ``False``.
    transforms : bool, optional
        Whether to include transform metadata. Default is ``False``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.

    Raises
    ------
    ValueError
        If ``config`` is not provided.
    """
    if args is not None:
        return check_handler(
            config=getattr(args, 'config', None),
            jobs=getattr(args, 'jobs', False),
            pipelines=getattr(args, 'pipelines', False),
            sources=getattr(args, 'sources', False),
            summary=getattr(args, 'summary', False),
            targets=getattr(args, 'targets', False),
            transforms=getattr(args, 'transforms', False),
            pretty=getattr(args, 'pretty', True),
        )

    if config is None:
        raise ValueError('config is required')

    cfg = load_pipeline_config(config, substitute=True)
    if summary:
        cli_io.emit_json(_pipeline_summary(cfg), pretty=True)
        return 0

    cli_io.emit_json(
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


def extract_handler(
    args: object | None = None,
    *,
    source_type: str | None = None,
    source: str | None = None,
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
    args : object | None, optional
        Parsed command-line arguments. If provided, other parameters are
        ignored. Default is ``None``.
    source_type : str | None, optional
        The type of the source (e.g., 'file', 'api', 'database'). Required if
        ``args`` is ``None``. Default is ``None``.
    source : str | None, optional
        The source identifier (e.g., path, URL, DSN). Required if ``args`` is
        ``None``. Default is ``None``.
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
    ValueError
        If ``source_type`` or ``source`` is not provided.
    """
    if args is not None:
        return extract_handler(
            source_type=getattr(args, 'source_type', None),
            source=getattr(args, 'source', None),
            format_hint=getattr(args, 'format', None)
            or getattr(args, 'target_format', None)
            or getattr(args, 'source_format', None),
            format_explicit=getattr(args, '_format_explicit', False),
            target=getattr(args, 'target', None),
            output=getattr(args, 'output', None),
            pretty=getattr(args, 'pretty', True),
        )

    if source_type is None or source is None:
        raise ValueError('source_type and source are required')

    explicit_format = format_hint if format_explicit else None

    if source == '-':
        text = cli_io.read_stdin_text()
        payload = cli_io.parse_text_payload(
            text,
            format_hint,
        )
        cli_io.emit_json(payload, pretty=pretty)

        return 0

    result = extract(
        source_type,
        source,
        file_format=explicit_format,
    )
    output_path = target or output

    cli_io.emit_or_write(
        result,
        output_path,
        pretty=pretty,
        success_message='Data extracted and saved to',
    )

    return 0


def load_handler(
    args: object | None = None,
    *,
    source: str | None = None,
    target_type: str | None = None,
    target: str | None = None,
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
    args : object | None, optional
        Parsed command-line arguments. If provided, other parameters are
        ignored. Default is ``None``.
    source : str | None, optional
        The source payload (e.g., path, inline data). Required if ``args`` is
        ``None``. Default is ``None``.
    target_type : str | None, optional
        The type of the target (e.g., 'file', 'database'). Required if ``args``
        is ``None``. Default is ``None``.
    target : str | None, optional
        The target destination (e.g., path, DSN). Required if ``args`` is
        ``None``. Default is ``None``.
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
    ValueError
        If ``source``, ``target_type``, or ``target`` is not provided.
    """
    if args is not None:
        return load_handler(
            source=getattr(args, 'source', None),
            target_type=getattr(args, 'target_type', None),
            target=getattr(args, 'target', None),
            source_format=getattr(args, 'source_format', None),
            target_format=getattr(args, 'target_format', None)
            or getattr(args, 'format', None),
            format_explicit=getattr(args, '_format_explicit', False),
            output=getattr(args, 'output', None),
            pretty=getattr(args, 'pretty', True),
        )

    if source is None or target_type is None or target is None:
        raise ValueError('source, target_type, and target are required')

    explicit_format = target_format if format_explicit else None

    # Allow piping into load.
    source_value = cast(
        str | Path | os.PathLike[str] | dict[str, Any] | list[dict[str, Any]],
        cli_io.resolve_cli_payload(
            source,
            format_hint=source_format,
            format_explicit=source_format is not None,
            hydrate_files=False,
        ),
    )

    # Allow piping out of load for file targets.
    if target_type == 'file' and target == '-':
        payload = cli_io.materialize_file_payload(
            source_value,
            format_hint=source_format,
            format_explicit=source_format is not None,
        )
        cli_io.emit_json(payload, pretty=pretty)
        return 0

    result = load(
        source_value,
        target_type,
        target,
        file_format=explicit_format,
    )

    output_path = output
    cli_io.emit_or_write(
        result,
        output_path,
        pretty=pretty,
        success_message='Load result saved to',
    )

    return 0


def render_handler(
    args: object | None = None,
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
    args : object | None, optional
        Parsed command-line arguments. If provided, other parameters are
        ignored. Default is ``None``.
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
    if args is not None:
        return render_handler(
            config=getattr(args, 'config', None),
            spec=getattr(args, 'spec', None),
            table=getattr(args, 'table', None),
            template=getattr(args, 'template', None),
            template_path=getattr(args, 'template_path', None),
            output=getattr(args, 'output', None),
            pretty=getattr(args, 'pretty', True),
            quiet=getattr(args, 'quiet', False),
        )

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
    sql_text = (
        '\n'.join(chunk.rstrip() for chunk in rendered_chunks).rstrip() + '\n'
    )
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
    args: object | None = None,
    *,
    config: str | None = None,
    job: str | None = None,
    pipeline: str | None = None,
    pretty: bool = True,
) -> int:
    """
    Execute an ETL job end-to-end from a pipeline YAML configuration.

    Parameters
    ----------
    args : object | None, optional
        Parsed command-line arguments. If provided, other parameters are
        ignored. Default is ``None``.
    config : str | None, optional
        Path to the pipeline YAML configuration. Required if ``args`` is
        ``None``. Default is ``None``.
    job : str | None, optional
        Name of the job to run. If not provided, runs the entire pipeline.
        Default is ``None``.
    pipeline : str | None, optional
        Alias for ``job``. Default is ``None``.
    pretty : bool, optional
        Whether to pretty-print output. Default is ``True``.

    Returns
    -------
    int
        Zero on success.

    Raises
    ------
    ValueError
        If ``config`` is not provided.
    """
    if args is not None:
        return run_handler(
            config=getattr(args, 'config', None),
            job=getattr(args, 'job', None),
            pipeline=getattr(args, 'pipeline', None),
            pretty=getattr(args, 'pretty', True),
        )

    if config is None:
        raise ValueError('config is required')

    cfg = load_pipeline_config(config, substitute=True)

    job_name = job or pipeline
    if job_name:
        result = run(job=job_name, config_path=config)
        cli_io.emit_json({'status': 'ok', 'result': result}, pretty=pretty)
        return 0

    cli_io.emit_json(_pipeline_summary(cfg), pretty=pretty)
    return 0


TransformOperations = Mapping[
    Literal['filter', 'map', 'select', 'sort', 'aggregate'],
    Any,
]


def transform_handler(
    args: object | None = None,
    *,
    source: str | None = None,
    operations: JSONData | str | None = None,
    target: str | None = None,
    source_format: str | None = None,
    target_format: str | None = None,
    pretty: bool = True,
    format_explicit: bool = False,
) -> int:
    """
    Transform data from a source.

    Parameters
    ----------
    args : object | None, optional
        Parsed command-line arguments. If provided, other parameters are
        ignored. Default is ``None``.
    source : str | None, optional
        The source payload (e.g., path, inline data). Required if ``args`` is
        ``None``. Default is ``None``.
    operations : JSONData | str | None, optional
        The transformation operations (inline JSON or path). Required if
        ``args`` is ``None``. Default is ``None``.
    target : str | None, optional
        The target destination (e.g., path). Default is ``None``.
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
        If ``source`` or ``operations`` is not provided.
    """
    if args is not None:
        return transform_handler(
            source=getattr(args, 'source', None),
            operations=getattr(args, 'operations', None),
            target=getattr(args, 'target', None),
            source_format=getattr(args, 'source_format', None),
            target_format=getattr(args, 'target_format', None),
            pretty=getattr(args, 'pretty', True),
            format_explicit=getattr(args, '_format_explicit', False),
        )

    if source is None or operations is None:
        raise ValueError('source and operations are required')

    format_hint: str | None = source_format
    format_explicit = format_hint is not None or format_explicit

    payload = cast(
        JSONData | str,
        cli_io.resolve_cli_payload(
            source,
            format_hint=format_hint,
            format_explicit=format_explicit,
        ),
    )

    operations_payload = cli_io.resolve_cli_payload(
        operations,
        format_hint=None,
        format_explicit=format_explicit,
    )
    if not isinstance(operations_payload, dict):
        raise ValueError('operations must resolve to a mapping of transforms')

    data = transform(payload, cast(TransformOperations, operations_payload))

    if target and target != '-':
        File.write_file(target, data, file_format=target_format)
        print(f'Data transformed and saved to {target}')
        return 0

    cli_io.emit_json(data, pretty=pretty)
    return 0


def validate_handler(
    args: object | None = None,
    *,
    source: str | None = None,
    rules: JSONData | str | None = None,
    source_format: str | None = None,
    target: str | None = None,
    format_explicit: bool = False,
    pretty: bool = True,
) -> int:
    """
    Validate data from a source.

    Parameters
    ----------
    args : object | None, optional
        Parsed command-line arguments. If provided, other parameters are
        ignored. Default is ``None``.
    source : str | None, optional
        The source payload (e.g., path, inline data). Required if ``args`` is
        ``None``. Default is ``None``.
    rules : JSONData | str | None, optional
        The validation rules (inline JSON or path). Required if ``args`` is
        ``None``. Default is ``None``.
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
        If ``source`` or ``rules`` is not provided.
    """
    if args is not None:
        return validate_handler(
            source=getattr(args, 'source', None),
            rules=getattr(args, 'rules', None),
            source_format=getattr(args, 'source_format', None),
            target=getattr(args, 'target', None),
            format_explicit=getattr(args, '_format_explicit', False),
            pretty=getattr(args, 'pretty', True),
        )

    if source is None or rules is None:
        raise ValueError('source and rules are required')

    format_hint: str | None = source_format
    payload = cast(
        JSONData | str,
        cli_io.resolve_cli_payload(
            source,
            format_hint=format_hint,
            format_explicit=format_explicit,
        ),
    )

    rules_payload = cli_io.resolve_cli_payload(
        rules,
        format_hint=None,
        format_explicit=format_explicit,
    )
    if not isinstance(rules_payload, dict):
        raise ValueError('rules must resolve to a mapping of field rules')

    field_rules = cast(Mapping[str, FieldRules], rules_payload)
    result = validate(payload, field_rules)

    target_path = target
    if target_path:
        validated_data = result.get('data')
        if validated_data is not None:
            cli_io.write_json_output(
                validated_data,
                target_path,
                success_message='Validation result saved to',
            )
        else:
            print(
                f'Validation failed, no data to save for {target_path}',
                file=sys.stderr,
            )
    else:
        cli_io.emit_json(result, pretty=pretty)

    return 0
