"""
:mod:`etlplus.cli.handlers` module.

Command handler functions for the ``etlplus`` command-line interface (CLI).
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
from pathlib import Path
from typing import Any
from typing import Literal
from typing import cast

from ..config import PipelineConfig
from ..config import load_pipeline_config
from ..enums import FileFormat
from ..extract import extract
from ..file import File
from ..load import load
from ..run import run
from ..transform import transform
from ..types import JSONData
from ..utils import json_type
from ..utils import print_json
from ..validate import validate

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'cmd_extract',
    'cmd_list',
    'cmd_load',
    'cmd_pipeline',
    'cmd_run',
    'cmd_transform',
    'cmd_validate',
]


# SECTION: INTERNAL CONSTANTS =============================================== #


# Standard output/error format behavior states
_FORMAT_ERROR_STATES = {'error', 'fail', 'strict'}
_FORMAT_SILENT_STATES = {'ignore', 'silent'}


# SECTION: CONSTANTS ======================================================== #


FORMAT_ENV_KEY = 'ETLPLUS_FORMAT_BEHAVIOR'


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _emit_behavioral_notice(
    message: str,
    behavior: str,
    *,
    quiet: bool,
) -> None:
    """
    Emit or raise format-behavior notices.

    Parameters
    ----------
    message : str
        Warning message describing the ignored ``--format`` flag.
    behavior : str
        Effective format-behavior mode derived from CLI options and env.
    quiet : bool
        Whether non-essential warnings should be suppressed.

    Raises
    ------
    ValueError
        If ``behavior`` maps to an error state.
    """
    if behavior in _FORMAT_ERROR_STATES:
        raise ValueError(message)
    if behavior in _FORMAT_SILENT_STATES or quiet:
        return
    print(f'Warning: {message}', file=sys.stderr)


def _emit_json(
    data: Any,
    *,
    pretty: bool,
) -> None:
    """
    Emit JSON to stdout honoring the pretty/compact preference.

    Parameters
    ----------
    data : Any
        Arbitrary JSON-serializable payload.
    pretty : bool
        When ``True`` pretty-print via :func:`print_json`; otherwise emit a
        compact JSON string.
    """
    if pretty:
        print_json(data)
        return

    dumped = json.dumps(
        data,
        ensure_ascii=False,
        separators=(',', ':'),
    )
    print(dumped)


def _format_behavior(
    strict: bool,
) -> str:
    """
    Return the effective format-behavior mode.

    Parameters
    ----------
    strict : bool
        Whether to enforce strict format behavior.

    Returns
    -------
    str
        The effective format-behavior mode.
    """
    if strict:
        return 'error'
    env_value = os.getenv(FORMAT_ENV_KEY, 'warn')
    return (env_value or 'warn').strip().lower()


def _handle_format_guard(
    *,
    io_context: Literal['source', 'target'],
    resource_type: str,
    format_explicit: bool,
    strict: bool,
    quiet: bool,
) -> None:
    """
    Warn or raise when --format is used alongside file resources.

    Parameters
    ----------
    io_context : Literal['source', 'target']
        Whether this is a source or target resource.
    resource_type : str
        The type of resource being processed.
    format_explicit : bool
        Whether the --format option was explicitly provided.
    strict : bool
        Whether to enforce strict format behavior.
    quiet : bool
        Whether to suppress warnings.
    """
    if resource_type != 'file' or not format_explicit:
        return
    message = (
        f'--format is ignored for file {io_context}s; '
        'inferred from filename extension.'
    )
    behavior = _format_behavior(strict)
    _emit_behavioral_notice(message, behavior, quiet=quiet)


def _infer_payload_format(
    text: str,
) -> str:
    """
    Infer JSON vs CSV from payload text.

    Parameters
    ----------
    text : str
        Incoming payload as plain text.

    Returns
    -------
    str
        ``'json'`` when the text starts with ``{``/``[``, else ``'csv'``.
    """
    stripped = text.lstrip()
    if stripped.startswith('{') or stripped.startswith('['):
        return 'json'
    return 'csv'


def _list_sections(
    cfg: PipelineConfig,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """
    Build sectioned metadata output for the list command.

    Parameters
    ----------
    cfg : PipelineConfig
        The loaded pipeline configuration.
    args : argparse.Namespace
        Parsed command-line arguments.

    Returns
    -------
    dict[str, Any]
        Metadata output for the list command.
    """
    sections: dict[str, Any] = {}
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


def _materialize_csv_payload(
    source: object,
) -> JSONData | str:
    """
    Return parsed CSV rows when ``source`` points at a CSV file.

    Parameters
    ----------
    source : object
        The source of data.

    Returns
    -------
    JSONData | str
        Parsed CSV rows or the original source if not a CSV file.
    """
    if not isinstance(source, str):
        return cast(JSONData, source)
    path = Path(source)
    if path.suffix.lower() != '.csv' or not path.is_file():
        return source
    return _read_csv_rows(path)


def _parse_text_payload(
    text: str,
    fmt: str | None,
) -> JSONData | str:
    """
    Parse JSON/CSV text into a Python payload.

    Parameters
    ----------
    text : str
        The input text payload.
    fmt : str | None
        Explicit format hint: 'json', 'csv', or None to infer.

    Returns
    -------
    JSONData | str
        The parsed payload as JSON data or raw text.
    """

    effective = (fmt or '').strip().lower() or _infer_payload_format(text)
    if effective == 'json':
        return cast(JSONData, json_type(text))
    if effective == 'csv':
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader]
    return text


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


def _presentation_flags(
    args: argparse.Namespace,
) -> tuple[bool, bool]:
    """Return presentation toggles from the parsed namespace.

    Parameters
    ----------
    args : argparse.Namespace
        Namespace produced by the CLI parser.

    Returns
    -------
    tuple[bool, bool]
        Pair of ``(pretty, quiet)`` flags with safe defaults.
    """
    return getattr(args, 'pretty', True), getattr(args, 'quiet', False)


def _read_csv_rows(
    path: Path,
) -> list[dict[str, str]]:
    """
    Read CSV rows into dictionaries.

    Parameters
    ----------
    path : Path
        Path to a CSV file.

    Returns
    -------
    list[dict[str, str]]
        List of dictionaries, each representing a row in the CSV file.
    """
    with path.open(newline='', encoding='utf-8') as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _read_stdin_text() -> str:
    """
    Return every character from ``stdin`` as a single string.

    Returns
    -------
    str
        Entire ``stdin`` contents.
    """
    return sys.stdin.read()


def _write_json_output(
    data: Any,
    output_path: str | None,
    *,
    success_message: str,
) -> bool:
    """
    Optionally persist JSON data to disk.

    Parameters
    ----------
    data : Any
        Data to write.
    output_path : str | None
        Path to write the output to. None to print to stdout.
    success_message : str
        Message to print upon successful write.

    Returns
    -------
    bool
        True if output was written to a file, False if printed to stdout.
    """
    if not output_path or output_path == '-':
        return False
    File(Path(output_path), FileFormat.JSON).write_json(data)
    print(f'{success_message} {output_path}')
    return True


# SECTION: FUNCTIONS ======================================================== #


def cmd_extract(
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
    pretty, quiet = _presentation_flags(args)

    _handle_format_guard(
        io_context='source',
        resource_type=args.source_type,
        format_explicit=getattr(args, '_format_explicit', False),
        strict=getattr(args, 'strict_format', False),
        quiet=quiet,
    )

    if args.source == '-':
        text = _read_stdin_text()
        payload = _parse_text_payload(text, getattr(args, 'format', None))
        _emit_json(payload, pretty=pretty)
        return 0

    if args.source_type == 'file':
        result = extract(args.source_type, args.source)
    else:
        result = extract(
            args.source_type,
            args.source,
            file_format=getattr(args, 'format', None),
        )

    _emit_json(result, pretty=pretty)
    return 0


def cmd_validate(
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
    pretty, quiet = _presentation_flags(args)

    source_type = getattr(args, 'source_type', None)
    if source_type is not None:
        _handle_format_guard(
            io_context='source',
            resource_type=source_type,
            format_explicit=getattr(args, '_format_explicit', False),
            strict=getattr(args, 'strict_format', False),
            quiet=quiet,
        )

    if args.source == '-':
        text = _read_stdin_text()
        payload = _parse_text_payload(
            text,
            getattr(args, 'source_format', None),
        )
    else:
        payload = _materialize_csv_payload(args.source)
    result = validate(payload, args.rules)

    output_path = getattr(args, 'output', None)
    if output_path:
        validated_data = result.get('data')
        if validated_data is not None:
            _write_json_output(
                validated_data,
                output_path,
                success_message='Validation result saved to',
            )
        else:
            print(
                f'Validation failed, no data to save for {output_path}',
                file=sys.stderr,
            )
    else:
        _emit_json(result, pretty=pretty)

    return 0


def cmd_transform(
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
    pretty, quiet = _presentation_flags(args)

    target_type = getattr(args, 'target_type', None)
    if target_type is not None:
        _handle_format_guard(
            io_context='target',
            resource_type=target_type,
            format_explicit=getattr(args, '_format_explicit', False),
            strict=getattr(args, 'strict_format', False),
            quiet=quiet,
        )

    if args.source == '-':
        text = _read_stdin_text()
        payload = _parse_text_payload(
            text,
            getattr(args, 'source_format', None),
        )
    else:
        payload = _materialize_csv_payload(args.source)

    data = transform(payload, args.operations)

    if not _write_json_output(
        data,
        getattr(args, 'output', None),
        success_message='Data transformed and saved to',
    ):
        _emit_json(data, pretty=pretty)

    return 0


def cmd_load(
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
    pretty, quiet = _presentation_flags(args)

    _handle_format_guard(
        io_context='target',
        resource_type=args.target_type,
        format_explicit=getattr(args, '_format_explicit', False),
        strict=getattr(args, 'strict_format', False),
        quiet=quiet,
    )

    # Allow piping into load.
    source_value: (
        str | Path | os.PathLike[str] | dict[str, Any] | list[dict[str, Any]]
    )
    if args.source == '-':
        text = _read_stdin_text()
        source_format = getattr(args, 'source_format', None)
        if source_format is None:
            source_format = getattr(args, 'source_format', None)
        source_value = cast(
            str | dict[str, Any] | list[dict[str, Any]],
            _parse_text_payload(
                text,
                source_format,
            ),
        )
    else:
        source_value = args.source

    # Allow piping out of load for file targets.
    if args.target_type == 'file' and args.target == '-':
        payload = _materialize_csv_payload(source_value)
        _emit_json(payload, pretty=pretty)
        return 0

    if args.target_type == 'file':
        result = load(source_value, args.target_type, args.target)
    else:
        result = load(
            source_value,
            args.target_type,
            args.target,
            file_format=getattr(args, 'format', None),
        )

    if not _write_json_output(
        result,
        getattr(args, 'output', None),
        success_message='Data loaded and saved to',
    ):
        _emit_json(result, pretty=pretty)

    return 0


def cmd_pipeline(
    args: argparse.Namespace,
) -> int:
    """
    Inspect or run a pipeline YAML configuration.

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

    if getattr(args, 'list', False) and not getattr(args, 'run', None):
        print_json({'jobs': _pipeline_summary(cfg)['jobs']})
        return 0

    run_job = getattr(args, 'run', None)
    if run_job:
        result = run(job=run_job, config_path=args.config)
        print_json({'status': 'ok', 'result': result})
        return 0

    print_json(_pipeline_summary(cfg))
    return 0


def cmd_list(args: argparse.Namespace) -> int:
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
    print_json(_list_sections(cfg, args))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
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
        print_json({'status': 'ok', 'result': result})
        return 0

    print_json(_pipeline_summary(cfg))
    return 0
