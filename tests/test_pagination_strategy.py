"""
Pagination strategy tests
=========================

Tiny tests to lock in pagination invariants for page/offset and cursor modes.

We mock etlplus.extract.extract to simulate API responses and monkeypatch
time.sleep to avoid delays. We drive the CLI entrypoint to exercise the same
code paths as real usage without network calls.

Maintainer note:
- Pagination logic was modularized into ``etlplus.api.pagination.paginate``
  which calls an internal ``_extract`` on each page fetch.
- The CLI may still call ``etlplus.cli.extract`` on some code paths.
- To keep tests hermetic after modularization, we patch both targets:
  ``cli_mod.extract`` and ``pmod._extract``.
"""
from __future__ import annotations

import json
import sys
import time
from typing import Any

import etlplus.api.client as cmod
import etlplus.api.pagination as pmod
import etlplus.cli as cli_mod
from etlplus.cli import main


def _write_pipeline(tmp_path, yaml_text: str) -> str:
    p = tmp_path / 'pipeline.yml'
    p.write_text(yaml_text, encoding='utf-8')
    return str(p)


def test_pagination_page_offset(monkeypatch, tmp_path, capsys):
    """
    Verify page/offset stops when the returned batch is smaller than page_size
    and aggregates all records across pages.
    """

    # Prepare output path
    out_path = tmp_path / 'out.json'

    # Minimal pipeline with API source using page/offset pagination
    pipeline_yaml = f"""
name: Page Offset Test
sources:
  - name: src
    type: api
    url: https://example.test/api
targets:
  - name: dest
    type: file
    format: json
    path: {out_path}
jobs:
  - name: api_page
    extract:
      source: src
      options:
        pagination:
          type: page
          page_param: page
          size_param: per_page
          page_size: 2
    load:
      target: dest
"""

    cfg = _write_pipeline(tmp_path, pipeline_yaml)

    # Mock extract to return 2 items for page 1 and 1 item for page 2
    def fake_extract(kind: str, _url: str, **kwargs: Any):
        assert kind == 'api'
        params = kwargs.get('params') or {}
        page = int(params.get('page', 1))
        per_page = int(params.get('per_page', 2))
        assert per_page == 2
        if page == 1:
            return [{'id': 1}, {'id': 2}]
        if page == 2:
            return [{'id': 3}]  # smaller than page_size -> stop
        return []

    # Patch both extract targets:
    # - cli_mod.extract: CLI may call extract directly for non-paginated paths
    #   and for historical consistency.
    # - pmod._extract: paginate() calls an internal _extract on each page;
    #   after modularization, mocking only cli_mod.extract won't intercept page
    #   fetches.
    monkeypatch.setattr(cli_mod, 'extract', fake_extract)
    monkeypatch.setattr(pmod, '_extract', fake_extract)
    monkeypatch.setattr(cmod, '_extract', fake_extract)
    monkeypatch.setattr(time, 'sleep', lambda _s: None)

    # Run CLI
    monkeypatch.setattr(
        sys,
        'argv',
        ['etlplus', 'pipeline', '--config', cfg, '--run', 'api_page'],
    )
    rc = main()
    assert rc == 0

    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload.get('status') == 'ok'

    # Output should contain 3 aggregated items
    data = json.loads(out_path.read_text(encoding='utf-8'))
    assert isinstance(data, list)
    assert [r['id'] for r in data] == [1, 2, 3]


def test_pagination_cursor(monkeypatch, tmp_path, capsys):
    """
    Verify cursor pagination follows next cursor until it is missing and that
    records_path is respected.
    """

    out_path = tmp_path / 'out_cur.json'

    pipeline_yaml = f"""
name: Cursor Test
sources:
  - name: src
    type: api
    url: https://example.test/api
targets:
  - name: dest
    type: file
    format: json
    path: {out_path}
jobs:
  - name: api_cursor
    extract:
      source: src
      options:
        pagination:
          type: cursor
          cursor_param: cursor
          cursor_path: next
          page_size: 2
          records_path: data
    load:
      target: dest
"""

    cfg = _write_pipeline(tmp_path, pipeline_yaml)

    # Mock extract('api', ...) to return cursor-driven pages
    def fake_extract(kind: str, _url: str, **kwargs: Any):
        assert kind == 'api'
        params = kwargs.get('params') or {}
        cur = params.get('cursor')
        limit = int(params.get('limit', 2))
        assert limit == 2
        if cur is None:
            return {'data': [{'id': 'a'}, {'id': 'b'}], 'next': 'tok1'}
        if cur == 'tok1':
            return {'data': [{'id': 'c'}], 'next': None}
        return {'data': [], 'next': None}

    # Patch both extract targets for the same reason as the page/offset test:
    # paginate() uses pmod._extract internally, while the CLI may use
    # cli_mod.extract in other code paths.
    monkeypatch.setattr(cli_mod, 'extract', fake_extract)
    monkeypatch.setattr(pmod, '_extract', fake_extract)
    monkeypatch.setattr(cmod, '_extract', fake_extract)
    monkeypatch.setattr(time, 'sleep', lambda _s: None)

    monkeypatch.setattr(
        sys,
        'argv',
        ['etlplus', 'pipeline', '--config', cfg, '--run', 'api_cursor'],
    )
    rc = main()
    assert rc == 0

    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload.get('status') == 'ok'

    data = json.loads(out_path.read_text(encoding='utf-8'))
    assert [r['id'] for r in data] == ['a', 'b', 'c']


def test_pagination_max_records_trim(monkeypatch, tmp_path):
    """
    Verify that max_records trims the aggregated results.
    """

    out_path = tmp_path / 'out_trim.json'

    pipeline_yaml = f"""
name: Trim Test
sources:
  - name: src
    type: api
    url: https://example.test/api
targets:
  - name: dest
    type: file
    format: json
    path: {out_path}
jobs:
  - name: api_trim
    extract:
      source: src
      options:
        pagination:
          type: page
          page_param: page
          size_param: per_page
          page_size: 3
          max_records: 2
    load:
      target: dest
"""

    cfg = _write_pipeline(tmp_path, pipeline_yaml)

    def fake_extract(kind: str, _url: str, **kwargs: Any):
        assert kind == 'api'
        params = kwargs.get('params') or {}
        page = int(params.get('page', 1))
        per_page = int(params.get('per_page', 3))
        assert per_page == 3
        if page == 1:
            return [{'id': 1}, {'id': 2}, {'id': 3}]
        if page == 2:
            return [{'id': 4}]
        return []

    # Patch both extract targets to ensure pagination calls are intercepted
    # regardless of whether they go through the CLI entry point or the
    # pagination module's internal fetch function.
    monkeypatch.setattr(cli_mod, 'extract', fake_extract)
    monkeypatch.setattr(pmod, '_extract', fake_extract)
    monkeypatch.setattr(cmod, '_extract', fake_extract)
    monkeypatch.setattr(time, 'sleep', lambda _s: None)

    monkeypatch.setattr(
        sys,
        'argv',
        ['etlplus', 'pipeline', '--config', cfg, '--run', 'api_trim'],
    )
    rc = main()
    assert rc == 0

    data = json.loads(out_path.read_text(encoding='utf-8'))
    # Should be trimmed to 2 items
    assert [r['id'] for r in data] == [1, 2]
