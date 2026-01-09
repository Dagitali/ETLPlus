"""
:mod:`etlplus.cli.handlers` module.

Command handler functions for the ``etlplus`` command-line interface (CLI).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any
from typing import cast

from ..config import PipelineConfig
from ..config import load_pipeline_config
from ..database import load_table_spec
from ..database import render_tables
from ..extract import extract
from ..load import load
from ..run import run
from ..transform import transform
from ..types import JSONData
from ..types import TemplateKey
from ..validate import validate
from .io import emit_json
from .io import explicit_cli_format
from .io import materialize_file_payload
from .io import parse_text_payload
from .io import presentation_flags
from .io import read_stdin_text
from .io import resolve_cli_payload
from .io import write_json_output

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
    args: argparse.Namespace,
) -> dict[str, Any]:
    """
    Build sectioned metadata output for the check command.

    Parameters
    ----------
    cfg : PipelineConfig
        The loaded pipeline configuration.
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    dict[str, Any]
        Metadata output for the check command.
    """
    sections: dict[str, Any] = {}
    if getattr(args, 'jobs', False):
        sections['jobs'] = _pipeline_summary(cfg)['jobs']
    if getattr(args, 'pipelines', False):
        sections['pipelines'] = [cfg.name]
    if getattr(args, 'sources', False):
        sections['sources'] = [src.name for src in cfg.sources]
    if getattr(args, 'targets', False):
        sections['targets'] = [tgt.name for tgt in cfg.targets]
    if getattr(args, 'transforms', False):
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
    args: argparse.Namespace,
) -> int:
    """
    Print requested pipeline sections from a YAML configuration.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    cfg = load_pipeline_config(args.config, substitute=True)
    if getattr(args, 'summary', False):
        emit_json(_pipeline_summary(cfg), pretty=True)
        return 0

    emit_json(_check_sections(cfg, args), pretty=True)
    return 0


def extract_handler(
    args: argparse.Namespace,
) -> int:
    """
    Extract data from a source.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    pretty, _ = presentation_flags(args)
    explicit_format = explicit_cli_format(args)

    if args.source == '-':
        text = read_stdin_text()
        payload = parse_text_payload(text, getattr(args, 'format', None))
        emit_json(payload, pretty=pretty)

        return 0

    result = extract(
        args.source_type,
        args.source,
        file_format=explicit_format,
    )
    output_path = getattr(args, 'target', None)
    if output_path is None:
        output_path = getattr(args, 'output', None)

    if not write_json_output(
        result,
        output_path,
        success_message='Data extracted and saved to',
    ):
        emit_json(result, pretty=pretty)

    return 0


def load_handler(
    args: argparse.Namespace,
) -> int:
    """
    Load data into a target.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    pretty, _ = presentation_flags(args)
    explicit_format = explicit_cli_format(args)

    # Allow piping into load.
    source_format = getattr(args, 'source_format', None)
    source_value = cast(
        str | Path | os.PathLike[str] | dict[str, Any] | list[dict[str, Any]],
        resolve_cli_payload(
            args.source,
            format_hint=source_format,
            format_explicit=source_format is not None,
            hydrate_files=False,
        ),
    )

    # Allow piping out of load for file targets.
    if args.target_type == 'file' and args.target == '-':
        payload = materialize_file_payload(
            source_value,
            format_hint=source_format,
            format_explicit=source_format is not None,
        )
        emit_json(payload, pretty=pretty)
        return 0

    result = load(
        source_value,
        args.target_type,
        args.target,
        file_format=explicit_format,
    )

    output_path = getattr(args, 'output', None)
    if not write_json_output(
        result,
        output_path,
        success_message='Load result saved to',
    ):
        emit_json(result, pretty=pretty)

    return 0


def render_handler(
    args: argparse.Namespace,
) -> int:
    """Render SQL DDL statements from table schema specs."""
    _, quiet = presentation_flags(args)

    template_value: TemplateKey = getattr(args, 'template', 'ddl') or 'ddl'
    template_path = getattr(args, 'template_path', None)
    table_filter = getattr(args, 'table', None)
    spec_path = getattr(args, 'spec', None)
    config_path = getattr(args, 'config', None)

    # If the provided template points to a file, treat it as a path override.
    file_override = template_path
    template_key: TemplateKey | None = template_value
    if template_path is None:
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

    output_path = getattr(args, 'output', None)
    if output_path and output_path != '-':
        Path(output_path).write_text(sql_text, encoding='utf-8')
        if not quiet:
            print(f'Rendered {len(specs)} schema(s) to {output_path}')
        return 0

    print(sql_text)
    return 0


def run_handler(
    args: argparse.Namespace,
) -> int:
    """
    Execute an ETL job end-to-end from a pipeline YAML configuration.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    cfg = load_pipeline_config(args.config, substitute=True)

    job_name = getattr(args, 'job', None) or getattr(args, 'pipeline', None)
    if job_name:
        result = run(job=job_name, config_path=args.config)
        emit_json({'status': 'ok', 'result': result}, pretty=True)
        return 0

    emit_json(_pipeline_summary(cfg), pretty=True)
    return 0


def transform_handler(
    args: argparse.Namespace,
) -> int:
    """
    Transform data from a source.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    pretty, _ = presentation_flags(args)
    format_hint: str | None = getattr(args, 'source_format', None)
    format_explicit: bool = format_hint is not None

    payload = cast(
        JSONData | str,
        resolve_cli_payload(
            args.source,
            format_hint=format_hint,
            format_explicit=format_explicit,
        ),
    )

    data = transform(payload, args.operations)

    if not write_json_output(
        data,
        getattr(args, 'target', None),
        success_message='Data transformed and saved to',
    ):
        emit_json(data, pretty=pretty)

    return 0


def validate_handler(
    args: argparse.Namespace,
) -> int:
    """
    Validate data from a source.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    int
        Zero on success.
    """
    pretty, _ = presentation_flags(args)
    format_explicit: bool = getattr(args, '_format_explicit', False)
    format_hint: str | None = getattr(args, 'source_format', None)
    payload = cast(
        JSONData | str,
        resolve_cli_payload(
            args.source,
            format_hint=format_hint,
            format_explicit=format_explicit,
        ),
    )
    result = validate(payload, args.rules)

    target_path = getattr(args, 'target', None)
    if target_path:
        validated_data = result.get('data')
        if validated_data is not None:
            write_json_output(
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
        emit_json(result, pretty=pretty)

    return 0
