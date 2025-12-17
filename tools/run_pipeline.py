#!/usr/bin/env python3
"""
run_pipeline.py script.

Execute jobs defined in a dbt-inspired YAML pipeline configuration.

Examples
--------------
- List jobs:
    python tools/run_pipeline.py --list

- Run a specific job (default config path):
    python tools/run_pipeline.py --job file_to_file_customers

- Run with a different config file:
    python tools/run_pipeline.py --config in/pipeline.yml \
        --job api_to_file_github_repos
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

from etlplus.enums import FileFormat
from etlplus.extract import extract
from etlplus.file import File
from etlplus.load import load
from etlplus.transform import transform
from etlplus.types import JSONDict
from etlplus.types import JSONList
from etlplus.types import StrAnyMap
from etlplus.types import StrStrMap
from etlplus.validate import validate

# SECTION: INTERNAL FUNCTIONS ============================================== #


def _deep_substitute(
    value: Any,
    vars_map: StrAnyMap,
    env_map: StrStrMap,
) -> Any:
    """
    Recursively substitute ${VAR} tokens using vars_map first, then env_map.

    - Only strings are substituted; other types are returned as-is.
    - Walks dicts and lists to apply substitutions throughout the structure.

    Parameters
    ----------
    value : Any
        The input value to perform substitutions on.
    vars_map : StrAnyMap
        Mapping of variable names to their replacement values.
    env_map : StrStrMap
        Mapping of environment variable names to their replacement values.

    Returns
    -------
    Any
        The value with substitutions applied.
    """

    if isinstance(value, str):
        out = value
        # simple token form: ${NAME}
        # replace repeatedly if multiple tokens present
        for name, replacement in vars_map.items():
            out = out.replace(f'${{{name}}}', str(replacement))
        for name, replacement in env_map.items():
            out = out.replace(f'${{{name}}}', str(replacement))
        return out

    if isinstance(value, dict):
        return {
            k: _deep_substitute(v, vars_map, env_map) for k, v in value.items()
        }

    if isinstance(value, list):
        return [_deep_substitute(v, vars_map, env_map) for v in value]

    return value


def _extract_from_source(
    source_cfg: JSONDict,
    overrides: JSONDict | None,
    vars_map: JSONDict,
    env_map: dict[str, str],
) -> Any:
    stype = str(source_cfg.get('type'))
    ov = overrides or {}

    # Resolve/merge values with substitution.
    def resolve(obj: Any) -> Any:
        return _deep_substitute(obj, vars_map, env_map)

    if stype == 'file':
        file_path = resolve(source_cfg.get('path'))
        fmt = ov.get('format', source_cfg.get('format', 'json'))
        return extract(stype, file_path, file_format=fmt)

    if stype == 'api':
        url = resolve(source_cfg.get('url'))
        base_params = resolve(source_cfg.get('params', {}))
        base_headers = resolve(source_cfg.get('headers', {}))
        # allow job-level overrides for params/headers
        params = {**base_params, **resolve(ov.get('params', {}))}
        headers = {**base_headers, **resolve(ov.get('headers', {}))}

        # -- Rate limit --------------------------------------------------- #
        rate = resolve(source_cfg.get('rate_limit', {}))
        rate = {**rate, **resolve(ov.get('rate_limit', {}))}
        sleep_s = rate.get('sleep_seconds')
        max_per_sec = rate.get('max_per_sec')
        if (
            sleep_s is None
            and isinstance(max_per_sec, (int, float))
            and max_per_sec > 0
        ):
            sleep_s = 1.0 / float(max_per_sec)
        if not isinstance(sleep_s, (int, float)):
            sleep_s = 0.0

        # -- Pagination --------------------------------------------------- #
        pagination = resolve(source_cfg.get('pagination', {}))
        pagination = {**pagination, **resolve(ov.get('pagination', {}))}
        ptype = str(pagination.get('type', '')).strip().lower()
        records_path = pagination.get('records_path')
        fallback_path = pagination.get('fallback_path')
        max_pages = pagination.get('max_pages')
        max_records = pagination.get('max_records')

        def _apply_sleep() -> None:
            if sleep_s and sleep_s > 0:
                time.sleep(float(sleep_s))

        def _coalesce_records(x: Any) -> JSONList:
            # Optionally drill into a dict via a simple dotted path to
            # find the records array.
            _missing = object()

            def _get_path(obj: Any, path: str | None) -> Any:
                if not isinstance(path, str) or not path:
                    return obj
                cur = obj
                for part in path.split('.'):
                    if isinstance(cur, dict) and part in cur:
                        cur = cur[part]
                    else:
                        return _missing
                return cur

            data = _get_path(x, records_path)
            if data is _missing:
                data = None

            if fallback_path and (
                data is None or (isinstance(data, list) and not data)
            ):
                fb = _get_path(x, fallback_path)
                if fb is not _missing:
                    data = fb

            if data is None and not records_path:
                data = x

            if isinstance(data, list):
                # Keep only dict items (normalize scalars into dicts)
                out: JSONList = []
                for item in data:
                    if isinstance(item, dict):
                        out.append(item)
                    else:
                        out.append({'value': item})
                return out
            if isinstance(data, dict):
                # Common API shape: { items: [...] }
                items = data.get('items')
                if isinstance(items, list):
                    return _coalesce_records(items)
                return [data]
            # Fallback
            return [{'value': data}]

        # No pagination requested → single call
        if not ptype:
            req_kwargs = {}
            if params:
                req_kwargs['params'] = params
            if headers:
                req_kwargs['headers'] = headers
            if 'timeout' in ov:
                req_kwargs['timeout'] = ov['timeout']
                return extract(
                    stype,
                    url,
                    file_format=None,
                    method='GET',
                    **req_kwargs,
                )

        results: JSONList = []
        pages = 0
        recs = 0

        if ptype in {'page', 'offset'}:
            page_param = pagination.get('page_param', 'page')
            size_param = pagination.get('size_param', 'per_page')
            start_page = int(pagination.get('start_page', 1))
            page_size = int(pagination.get('page_size', 100))

            current = start_page
            while True:
                req_params = dict(params)
                req_params[page_param] = current
                req_params[size_param] = page_size

                req_kwargs = {'params': req_params}
                if headers:
                    req_kwargs['headers'] = headers
                if 'timeout' in ov:
                    req_kwargs['timeout'] = ov['timeout']

                page_data = extract(
                    stype,
                    url,
                    file_format=None,
                    method='GET',
                    **req_kwargs,
                )
                batch = _coalesce_records(page_data)
                results.extend(batch)
                pages += 1
                recs += len(batch)

                # Stop conditions
                if len(batch) < page_size:
                    break
                if isinstance(max_pages, int) and pages >= max_pages:
                    break
                if isinstance(max_records, int) and recs >= max_records:
                    results = results[:max_records]
                    break

                _apply_sleep()
                current += 1

            return results

        if ptype == 'cursor':
            cursor_param = pagination.get('cursor_param', 'cursor')
            cursor_path = pagination.get('cursor_path')  # dotted path
            page_size = int(pagination.get('page_size', 100))
            cursor = pagination.get('start_cursor')

            while True:
                req_params = dict(params)
                if cursor is not None:
                    req_params[cursor_param] = cursor
                if page_size:
                    req_params.setdefault('limit', page_size)

                req_kwargs = {'params': req_params}
                if headers:
                    req_kwargs['headers'] = headers
                if 'timeout' in ov:
                    req_kwargs['timeout'] = ov['timeout']

                page_data = extract(
                    stype,
                    url,
                    file_format=None,
                    method='GET',
                    **req_kwargs,
                )
                batch = _coalesce_records(page_data)
                results.extend(batch)
                pages += 1
                recs += len(batch)

                # Derive next cursor
                next_cursor = None
                if isinstance(cursor_path, str) and cursor_path:
                    # Try to find next cursor in response dict
                    if isinstance(page_data, dict):
                        cur: Any = page_data
                        for part in cursor_path.split('.'):
                            if isinstance(cur, dict):
                                cur = cur.get(part)
                            else:
                                cur = None
                                break
                        if isinstance(cur, (str, int)):
                            next_cursor = cur

                # Stop conditions
                if not next_cursor or len(batch) == 0:
                    break
                if isinstance(max_pages, int) and pages >= max_pages:
                    break
                if isinstance(max_records, int) and recs >= max_records:
                    results = results[:max_records]
                    break

                _apply_sleep()
                cursor = next_cursor

            return results

        # Unknown pagination type → single request fallback
        req_kwargs = {}
        if params:
            req_kwargs['params'] = params
        if headers:
            req_kwargs['headers'] = headers
        if 'timeout' in ov:
            req_kwargs['timeout'] = ov['timeout']
        return extract(
            stype,
            url,
            file_format=None,
            method='GET',
            **req_kwargs,
        )

    if stype == 'database':
        conn = resolve(source_cfg.get('connection_string', ''))
        # extract() currently returns a placeholder for databases
        return extract(stype, conn, method='GET')

    raise ValueError(f'Unsupported source type: {stype}')


def _index_by_name(
    objs: JSONList,
) -> dict[str, JSONDict]:
    return {o['name']: o for o in objs}


def _load_to_target(
    data: Any,
    target_cfg: JSONDict,
    overrides: JSONDict | None,
    vars_map: JSONDict,
    env_map: dict[str, str],
) -> Any:
    ttype = str(target_cfg.get('type'))
    ov = overrides or {}

    def resolve(obj: Any) -> Any:
        return _deep_substitute(obj, vars_map, env_map)

    if ttype == 'file':
        path = resolve(ov.get('path', target_cfg.get('path')))
        fmt = ov.get('format', target_cfg.get('format', 'json'))
        return load(data, ttype, path, file_format=fmt)

    if ttype == 'api':
        url = resolve(ov.get('url', target_cfg.get('url')))
        method = ov.get('method', target_cfg.get('method', 'post'))
        headers = resolve(
            {
                **target_cfg.get('headers', {}),
                **ov.get('headers', {}),
            },
        )
        kwargs: JSONDict = {}
        if headers:
            kwargs['headers'] = headers
        if 'timeout' in ov:
            kwargs['timeout'] = ov['timeout']
        return load(data, ttype, url, method=method, **kwargs)

    if ttype == 'database':
        conn = resolve(
            ov.get(
                'connection_string',
                target_cfg.get('connection_string', ''),
            ),
        )
        return load(data, ttype, conn)

    raise ValueError(f'Unsupported target type: {ttype}')


# SECTION: FUNCTIONS ======================================================== #


def main(
    argv: list[str] | None = None,
) -> int:
    """
    Run ETLPlus jobs from a YAML pipeline configuration.

    Parameters
    ----------
    argv : list[str] | None, optional
        List of command-line arguments to parse. If None, defaults to
        sys.argv[1:].

    Returns
    -------
    int
        Exit code (0 for success, non-zero for errors).
    """
    ap = argparse.ArgumentParser(
        description='Run ETLPlus jobs from a YAML pipeline config',
    )
    ap.add_argument(
        '--config',
        default='in/pipeline.yml',
        help='Path to YAML config file',
    )
    ap.add_argument('--job', help='Job name to run')
    ap.add_argument(
        '--list',
        action='store_true',
        help='List available job names and exit',
    )
    args = ap.parse_args(argv)

    config_path = Path(args.config)
    if not config_path.exists():
        ap.error(f'Config not found: {config_path}')

    cfg = File(config_path, FileFormat.YAML).read_yaml()
    if not isinstance(cfg, dict):
        ap.error('Config root must be a mapping (YAML object)')

    # Vars and env
    vars_map: JSONDict = cfg.get('vars', {}) or {}
    profile = cfg.get('profile', {}) or {}
    profile_env: dict[str, str] = profile.get('env', {}) or {}

    # Compose env: profile.env overrides current environment if the same key
    # also exists in os.environ.
    env_map: dict[str, str] = dict(os.environ)
    env_map.update({k: str(v) for k, v in profile_env.items()})

    # Index entities by name
    sources_by_name = _index_by_name(cfg.get('sources', []) or [])
    targets_by_name = _index_by_name(cfg.get('targets', []) or [])
    validations = cfg.get('validations', {}) or {}
    transforms = cfg.get('transforms', {}) or {}

    jobs = cfg.get('jobs', []) or []
    job_names = [j.get('name') for j in jobs]

    if args.list:
        print(json.dumps({'jobs': job_names}, indent=2))
        return 0

    if not args.job:
        ap.error('--job is required (or use --list)')

    # Find job
    job = next((j for j in jobs if j.get('name') == args.job), None)
    if not job:
        ap.error(f'Job not found: {args.job}')

    # Extract
    extract_cfg = job.get('extract', {})
    source_name = extract_cfg.get('source')
    if source_name not in sources_by_name:
        ap.error(f'Unknown source: {source_name}')
    source_obj = sources_by_name[source_name]
    extract_overrides = extract_cfg.get('options', {})
    data = _extract_from_source(
        source_obj,
        extract_overrides,
        vars_map,
        env_map,
    )

    # Validate (optional) with severity/phase.
    if 'validate' in job:
        vcfg = job['validate'] or {}
        ruleset_name = vcfg.get('ruleset')
        severity = str(vcfg.get('severity', 'error')).lower()
        phase = str(vcfg.get('phase', 'before_transform')).lower()
        rules = validations.get(ruleset_name, {})

        def _handle_validation(payload: Any) -> Any:
            res = validate(payload, rules)
            if res.get('valid', False):
                return res['data']
            msg = json.dumps(
                {'status': 'validation_failed', 'result': res},
                indent=2,
            )
            if severity == 'warn':
                print(msg)
                return payload
            # severity == error (default)
            print(msg)
            raise SystemExit(1)

        if phase in {'before_transform', 'both'}:
            data = _handle_validation(data)

    # Transform (optional).
    if 'transform' in job:
        pipeline_name = job['transform'].get('pipeline')
        operations = transforms.get(pipeline_name, {})
        data = transform(data, operations)

    if 'validate' in job:
        vcfg = job['validate'] or {}
        ruleset_name = vcfg.get('ruleset')
        severity = str(vcfg.get('severity', 'error')).lower()
        phase = str(vcfg.get('phase', 'before_transform')).lower()
        rules = validations.get(ruleset_name, {})
        if phase in {'after_transform', 'both'}:
            res = validate(data, rules)
            if not res.get('valid', False):
                msg = json.dumps(
                    {'status': 'validation_failed', 'result': res},
                    indent=2,
                )
                if severity == 'warn':
                    print(msg)
                else:
                    print(msg)
                    raise SystemExit(1)

    # Load.
    load_cfg = job.get('load', {})
    target_name = load_cfg.get('target')
    if target_name not in targets_by_name:
        ap.error(f'Unknown target: {target_name}')
    target_obj = targets_by_name[target_name]
    load_overrides = load_cfg.get('overrides', {})

    result = _load_to_target(
        data,
        target_obj,
        load_overrides,
        vars_map,
        env_map,
    )
    print(
        json.dumps(
            {'status': 'ok', 'result': result},
            indent=2,
            ensure_ascii=False,
        ),
    )
    return 0


# SECTION: MAIN EXECUTION =================================================== #


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
