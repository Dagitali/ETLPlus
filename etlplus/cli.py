"""
ETLPlus Command-Line Interface
==============================

Entry point for the ``etlplus`` CLI.

This module wires subcommands via ``argparse`` using
``set_defaults(func=...)`` so dispatch is clean and extensible.

Subcommands
-----------
- ``extract``: extract data from files, databases, or REST APIs
- ``validate``: validate data against rules
- ``transform``: transform records
- ``load``: load data to files, databases, or REST APIs
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from textwrap import dedent
from typing import Any
from typing import cast
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from etlplus import __version__
from etlplus.api.client import EndpointClient
from etlplus.api.client import PaginationConfig
from etlplus.config import load_pipeline_config
from etlplus.extract import extract
from etlplus.load import load
from etlplus.transform import transform
from etlplus.validate import validate
from etlplus.validation.utils import maybe_validate as _maybe_validate


# SECTION: PROTECTED FUNCTIONS ============================================== #


def _json_type(option: str) -> Any:
    """
    Argparse ``type=`` hook that parses a JSON string.

    Parameters
    ----------
    option
        Raw CLI string to parse as JSON.

    Returns
    -------
    Any
        Parsed JSON value.

    Raises
    ------
    argparse.ArgumentTypeError
        If the input cannot be parsed as JSON.
    """

    try:
        return json.loads(option)
    except json.JSONDecodeError as e:  # pragma: no cover - argparse path
        raise argparse.ArgumentTypeError(
            f'Invalid JSON: {e.msg} (pos {e.pos})',
        ) from e


def _print_json(obj: Any) -> None:
    """
    Pretty-print JSON to stdout using UTF-8 without ASCII escaping.

    Parameters
    ----------
    obj
        Object to serialize as JSON.
    """

    print(json.dumps(obj, indent=2, ensure_ascii=False))


def _write_json(obj: Any, out: str | Path) -> None:
    """
    Write JSON to ``out``, creating parent dirs as needed.

    Parameters
    ----------
    obj
        Object to serialize as JSON.
    out : str | Path
        Output file path.
    """

    path = Path(out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, indent=2, ensure_ascii=False),
        encoding='utf-8',
    )


# SECTION: FUNCTIONS ======================================================== #


# -- Command Handlers -- #


def cmd_extract(args: argparse.Namespace) -> int:
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

    data = extract(args.source_type, args.source, format=args.format)
    if args.output:
        _write_json(data, args.output)
        print(f'Data extracted and saved to {args.output}')
    else:
        _print_json(data)

    return 0


def cmd_validate(args: argparse.Namespace) -> int:
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

    # ``args.rules`` already parsed by ``_json_type`` (defaults to {}).
    result = validate(args.source, args.rules)
    _print_json(result)

    return 0


def cmd_transform(args: argparse.Namespace) -> int:
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

    # ``args.operations`` already parsed by ``_json_type`` (defaults to {}).
    data = transform(args.source, args.operations)
    if args.output:
        _write_json(data, args.output)
        print(f'Data transformed and saved to {args.output}')
    else:
        _print_json(data)

    return 0


def cmd_load(args: argparse.Namespace) -> int:
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

    result = load(
        args.source,
        args.target_type,
        args.target,
        format=args.format,
    )
    _print_json(result)

    return 0


def cmd_pipeline(args: argparse.Namespace) -> int:
    """
    Inspect or run a pipeline YAML configuration.

    --list prints job names; --run JOB executes a job end-to-end.

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

    def _merge(
        a: dict[str, Any] | None,
        b: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Shallow merge two dicts, later values override earlier."""
        out: dict[str, Any] = {}
        if a:
            out.update(a)
        if b:
            out.update(b)
        return out

    def _build_req_kwargs(
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        timeout: float | int | None = None,
    ) -> dict[str, Any]:
        kw: dict[str, Any] = {}
        if params:
            kw['params'] = params
        if headers:
            kw['headers'] = headers
        if timeout is not None:
            kw['timeout'] = timeout
        return kw

    # List mode
    if getattr(args, 'list', False) and not getattr(args, 'run', None):
        jobs = [j.name for j in cfg.jobs if j.name]
        _print_json({'jobs': jobs})
        return 0

    # Run mode
    run_job = getattr(args, 'run', None)
    if run_job:
        # Lookup job by name
        job = next((j for j in cfg.jobs if j.name == run_job), None)
        if not job:
            raise ValueError(f'Job not found: {run_job}')

        # Index sources/targets by name
        sources_by_name = {getattr(s, 'name', None): s for s in cfg.sources}
        targets_by_name = {getattr(t, 'name', None): t for t in cfg.targets}

        # Helper: coalesce records into list[dict].
        def _coalesce_records(x: Any, records_path: str | None) -> list[dict]:
            def _get_path(obj: Any, path: str) -> Any:
                cur = obj
                for part in path.split('.'):  # simple dotted path
                    if isinstance(cur, dict):
                        cur = cur.get(part)
                    else:
                        return None
                return cur

            data = x
            if isinstance(records_path, str) and records_path:
                data = _get_path(x, records_path)

            if isinstance(data, list):
                out: list[dict] = []
                for item in data:
                    if isinstance(item, dict):
                        out.append(item)
                    else:
                        out.append({'value': item})
                return out
            if isinstance(data, dict):
                items = data.get('items')
                if isinstance(items, list):
                    return _coalesce_records(items, None)
                return [data]
            return [{'value': data}]

        # Extract.
        if not job.extract:
            raise ValueError('Job missing "extract" section')
        source_name = job.extract.source
        if source_name not in sources_by_name:
            raise ValueError(f'Unknown source: {source_name}')
        source_obj = sources_by_name[source_name]
        ex_opts: dict[str, Any] = job.extract.options or {}

        data: Any
        stype = getattr(source_obj, 'type', None)
        match stype:
            case 'file':
                path = getattr(source_obj, 'path', None)
                fmt = ex_opts.get('format') or getattr(
                    source_obj, 'format', 'json',
                )
                if not path:
                    raise ValueError('File source missing "path"')
                data = extract('file', path, format=fmt)
            case 'database':
                conn = getattr(source_obj, 'connection_string', '')
                data = extract('database', conn)
            case 'api':
                # Build URL, params, headers, pagination, rate_limit
                url: str | None = getattr(source_obj, 'url', None)
                params: dict[str, Any] = dict(
                    getattr(source_obj, 'params', {}) or {},
                )
                headers: dict[str, str] = dict(
                    getattr(source_obj, 'headers', {}) or {},
                )
                pagination = getattr(source_obj, 'pagination', None)
                rate_limit = getattr(source_obj, 'rate_limit', None)

                api_name = getattr(source_obj, 'api', None)
                endpoint_name = getattr(source_obj, 'endpoint', None)
                if api_name and endpoint_name:
                    api_cfg = cfg.apis.get(api_name)
                    if not api_cfg:
                        raise ValueError(f'API not defined: {api_name}')
                    ep = api_cfg.endpoints.get(endpoint_name)
                    if not ep:
                        raise ValueError(
                            f'Endpoint "{endpoint_name}" not defined in API '
                            f'"{api_name}"',
                        )
                    # Compose and inherit
                    base = api_cfg.base_url.rstrip('/')
                    path = ep.path.lstrip('/')
                    url = f'{base}/{path}'
                    params = {**ep.params, **params}
                    headers = {**api_cfg.headers, **headers}
                    pagination = pagination or ep.pagination
                    rate_limit = rate_limit or ep.rate_limit

                # Apply overrides from job.extract.options.
                params = _merge(params, ex_opts.get('params'))
                headers = _merge(headers, ex_opts.get('headers'))
                timeout = ex_opts.get('timeout')
                pag_ov = ex_opts.get('pagination') or {}
                rl_ov = ex_opts.get('rate_limit') or {}

                # Compute rate limit sleep using helper
                sleep_s = \
                    EndpointClient.compute_sleep_seconds(rate_limit, rl_ov)

                # Pagination params
                ptype = None
                records_path = None
                max_pages = None
                max_records = None
                if pagination:
                    ptype = (pagination.type or '').strip().lower()
                    records_path = pagination.records_path
                    max_pages = pagination.max_pages
                    max_records = pagination.max_records
                # Override with job-level
                if pag_ov:
                    ptype = (pag_ov.get('type') or ptype or '').strip().lower()
                    records_path = pag_ov.get('records_path', records_path)
                    max_pages = pag_ov.get('max_pages', max_pages)
                    max_records = pag_ov.get('max_records', max_records)

                # Delegate to pagination helper
                pag_cfg: dict[str, Any] | None = None
                if ptype:
                    pag_cfg = {
                        'type': ptype,
                        'records_path': records_path,
                        'max_pages': max_pages,
                        'max_records': max_records,
                    }
                    if ptype in {'page', 'offset'}:
                        page_param = (
                            pag_ov.get('page_param') if pag_ov else None
                        )
                        size_param = (
                            pag_ov.get('size_param') if pag_ov else None
                        )
                        start_page = (
                            pag_ov.get('start_page') if pag_ov else None
                        )
                        page_size = (
                            pag_ov.get('page_size') if pag_ov else None
                        )
                        if pagination:
                            page_param = (
                                page_param or pagination.page_param or 'page'
                            )
                            size_param = (
                                size_param
                                or pagination.size_param
                                or 'per_page'
                            )
                            start_page = (
                                start_page or pagination.start_page or 1
                            )
                            page_size = (
                                page_size or pagination.page_size or 100
                            )
                        pag_cfg.update(
                            {
                                'page_param': str(page_param or 'page'),
                                'size_param': str(size_param or 'per_page'),
                                'start_page': int(start_page or 1),
                                'page_size': int(page_size or 100),
                            },
                        )
                    elif ptype == 'cursor':
                        cursor_param = (
                            pag_ov.get('cursor_param') if pag_ov else None
                        )
                        cursor_path = (
                            pag_ov.get('cursor_path') if pag_ov else None
                        )
                        page_size = pag_ov.get('page_size') if pag_ov else None
                        start_cursor = None
                        if pagination:
                            cursor_param = (
                                cursor_param
                                or pagination.cursor_param
                                or 'cursor'
                            )
                            cursor_path = (
                                cursor_path or pagination.cursor_path
                            )
                            page_size = (
                                page_size or pagination.page_size or 100
                            )
                            start_cursor = pagination.start_cursor
                        pag_cfg.update(
                            {
                                'cursor_param': str(cursor_param or 'cursor'),
                                'cursor_path': cursor_path,
                                'page_size': int(page_size or 100),
                                'start_cursor': start_cursor,
                            },
                        )

                if not url:
                    raise ValueError('API source missing URL')
                # Use instance-based pagination via EndpointClient.
                parts = urlsplit(url)
                base = urlunsplit((parts.scheme, parts.netloc, '', '', ''))
                client = EndpointClient(base_url=base, endpoints={})
                data = client.paginate_url(
                    url,
                    params,
                    headers,
                    timeout,
                    cast(PaginationConfig | None, pag_cfg),
                    sleep_seconds=sleep_s,
                )
            case _:
                raise ValueError(f'Unsupported source type: {stype}')

        # DRY: unified validation helper (pre/post transform)
        val_ref = job.validate
        enabled_validation = val_ref is not None
        if enabled_validation:
            # Type narrowing for static checkers
            assert val_ref is not None
            rules = cfg.validations.get(val_ref.ruleset, {})
            severity = (
                (val_ref.severity or 'error').lower()
            )
            phase = (
                (val_ref.phase or 'before_transform').lower()
            )
        else:
            rules = {}
            severity = 'error'
            phase = 'before_transform'

        # Pre-transform validation (if configured)
        data = _maybe_validate(
            data,
            'before_transform',
            enabled=enabled_validation,
            rules=rules,
            phase=phase,
            severity=severity,
            validate_fn=validate,  # type: ignore[arg-type]
            print_json_fn=_print_json,
        )

        # Transform (optional).
        if job.transform:
            ops: Any = cfg.transforms.get(job.transform.pipeline, {})
            data = transform(data, ops)

        # Post-transform validation (if configured)
        data = _maybe_validate(
            data,
            'after_transform',
            enabled=enabled_validation,
            rules=rules,
            phase=phase,
            severity=severity,
            validate_fn=validate,  # type: ignore[arg-type]
            print_json_fn=_print_json,
        )

        # Load.
        if not job.load:
            raise ValueError('Job missing "load" section')
        target_name = job.load.target
        if target_name not in targets_by_name:
            raise ValueError(f'Unknown target: {target_name}')
        target_obj = targets_by_name[target_name]
        overrides = job.load.overrides or {}

        ttype = getattr(target_obj, 'type', None)
        match ttype:
            case 'file':
                path = (
                    overrides.get('path')
                    or getattr(target_obj, 'path', None)
                )
                fmt = overrides.get('format') or getattr(
                    target_obj, 'format', 'json',
                )
                if not path:
                    raise ValueError('File target missing "path"')
                result = load(data, 'file', path, format=fmt)
            case 'api':
                url = overrides.get('url') or getattr(target_obj, 'url', None)
                method = overrides.get('method') or getattr(
                    target_obj, 'method', 'post',
                )
                headers = {
                    **(getattr(target_obj, 'headers', {}) or {}),
                    **(overrides.get('headers') or {}),
                }
                kwargs: dict[str, Any] = {}
                if headers:
                    kwargs['headers'] = headers
                if 'timeout' in overrides:
                    kwargs['timeout'] = overrides['timeout']
                if not url:
                    raise ValueError('API target missing "url"')
                result = load(data, 'api', url, method=method, **kwargs)
            case 'database':
                conn = overrides.get('connection_string') or getattr(
                    target_obj, 'connection_string', '',
                )
                result = load(data, 'database', str(conn))
            case _:
                raise ValueError(f'Unsupported target type: {ttype}')

        _print_json({'status': 'ok', 'result': result})
        return 0

    # Default: print summary
    summary = {
        'name': cfg.name,
        'version': cfg.version,
        'sources': [getattr(s, 'name', None) for s in cfg.sources],
        'targets': [getattr(t, 'name', None) for t in cfg.targets],
        'jobs': [j.name for j in cfg.jobs if j.name],
    }
    _print_json(summary)

    return 0


# -- Parser -- #


def create_parser() -> argparse.ArgumentParser:
    """
    Create the argument parser for the CLI.

    Returns
    -------
    argparse.ArgumentParser
        Configured parser with subcommands for the CLI.
    """
    parser = argparse.ArgumentParser(
        prog='etlplus',
        description=dedent(
            """
            ETLPlus — A Swiss Army knife for simple ETL operations.

            Provide a subcommand and options. Examples:

              etlplus extract file data.csv --format csv -o out.json
              etlplus validate data.json --rules '{"required": ['id]'}'
              etlplus transform data.json --operations '{"select": ['id]'}'
              etlplus load data.json file output.json --format json
            """,
        ).strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        # formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        '-V', '--version',
        action='version',
        version=f'%(prog)s {__version__}',
    )

    subparsers = parser.add_subparsers(
        dest='command',
        help='Available commands',
    )

    # Define "extract" command.
    extract_parser = subparsers.add_parser(
        'extract',
        help=(
            'Extract data from sources (files, databases, REST APIs)'
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    extract_parser.add_argument(
        'source_type',
        choices=['file', 'database', 'api'],
        help='Type of source to extract from',
    )
    extract_parser.add_argument(
        'source',
        help=(
            'Source location (file path, database connection string, or '
            'API URL)'
        ),
    )
    extract_parser.add_argument(
        '-o', '--output',
        help='Output file to save extracted data (JSON format)',
    )
    extract_parser.add_argument(
        '--format',
        choices=['json', 'csv', 'xml'],
        default='json',
        help='Format of the source file to extract (default: json)',
    )
    extract_parser.set_defaults(func=cmd_extract)

    # Define "validate" command.
    validate_parser = subparsers.add_parser(
        'validate',
        help='Validate data from sources',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    validate_parser.add_argument(
        'source',
        help='Data source to validate (file path or JSON string)',
    )
    validate_parser.add_argument(
        '--rules',
        type=_json_type,
        default={},
        help='Validation rules as JSON string',
    )
    validate_parser.set_defaults(func=cmd_validate)

    # Define "transform" command.
    transform_parser = subparsers.add_parser(
        'transform',
        help='Transform data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    transform_parser.add_argument(
        'source',
        help='Data source to transform (file path or JSON string)',
    )
    transform_parser.add_argument(
        '--operations',
        type=_json_type,
        default={},
        help='Transformation operations as JSON string',
    )
    transform_parser.add_argument(
        '-o', '--output',
        help='Output file to save transformed data',
    )
    transform_parser.set_defaults(func=cmd_transform)

    # Define "load" command.
    load_parser = subparsers.add_parser(
        'load',
        help='Load data to targets (files, databases, REST APIs)',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    load_parser.add_argument(
        'source',
        help='Data source to load (file path or JSON string)',
    )
    load_parser.add_argument(
        'target_type',
        choices=['file', 'database', 'api'],
        help='Type of target to load to',
    )
    load_parser.add_argument(
        'target',
        help=(
            'Target location (file path, database connection string, or '
            'API URL)'
        ),
    )
    load_parser.add_argument(
        '--format',
        choices=['json', 'csv'],
        default='json',
        help='Format for the target file to load (default: json)',
    )
    load_parser.set_defaults(func=cmd_load)

    # Define "pipeline" command (reads YAML config).
    pipe_parser = subparsers.add_parser(
        'pipeline',
        help='Inspect pipeline YAML (list jobs)',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    pipe_parser.add_argument(
        '--config',
        required=True,
        help='Path to pipeline YAML configuration file',
    )
    pipe_parser.add_argument(
        '--list',
        action='store_true',
        help='List available job names and exit',
    )
    pipe_parser.add_argument(
        '--run',
        metavar='JOB',
        help='Run a specific job by name',
    )
    pipe_parser.set_defaults(func=cmd_pipeline)

    return parser


# -- Main -- #


def main(argv: list[str] | None = None) -> int:
    """
    Main entry point for the CLI.

    Parameters
    ----------
    argv : list[str] | None, optional
        List of command-line arguments. If ``None``, uses ``sys.argv``.

    Returns
    -------
    int
        Zero on success, non-zero on error.

    Notes
    -----
    This function prints results to stdout and errors to stderr.
    """
    parser = create_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    try:
        # Prefer argparse's dispatch to avoid duplicating logic.
        func = getattr(args, 'func', None)
        if callable(func):
            return int(func(args))

        # Fallback: no subcommand function bound.
        parser.print_help()
        return 0

    except KeyboardInterrupt:
        # Conventional exit code for SIGINT
        return 130

    except (OSError, TypeError, ValueError) as e:
        print(f'Error: {e}', file=sys.stderr)
        return 1
