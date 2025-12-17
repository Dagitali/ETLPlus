"""
:mod:`tests.integration.test_i_pagination_strategy` module.

Integration tests for pagination strategies. We mock API extraction for both
page/offset and cursor modes and drive the CLI entry point to exercise the
public path under real configuration semantics.

Notes
-----
- Pagination logic resides on ``EndpointClient.paginate_url``; patching the
    RequestManager ``request_once`` helper suffices to intercept page fetches.
- Some legacy paths still use ``cli_mod.extract``; we patch both for safety.
- ``time.sleep`` is neutralized to keep tests fast and deterministic.
"""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

import etlplus.api.request_manager as rmod
import etlplus.cli as cli_mod
from etlplus.cli import main
from etlplus.config.pipeline import PipelineConfig
from tests.integration.conftest import FakeEndpointClientProtocol

# SECTION: HELPERS ========================================================== #


@dataclass(slots=True)
class PageScenario:
    """Test scenario for page/offset pagination."""

    name: str
    page_size: int
    pages: list[list[dict[str, int]]]
    expected_ids: list[int]
    max_records: int | None = None


def _write_pipeline(
    tmp_path: Path,
    yaml_text: str,
) -> str:
    """
    Write a temporary pipeline.yml file and return its path.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory provided by pytest.
    yaml_text : str
        YAML configuration content to write.

    Returns
    -------
    str
        String path to the written pipeline.yml file.
    """
    p = tmp_path / 'pipeline.yml'
    p.write_text(yaml_text, encoding='utf-8')
    return str(p)


# SECTION: TESTS ============================================================ #


class TestPaginationStrategies:
    """Integration test suite for pagination strategies."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Disable time.sleep to keep pagination tests fast and deterministic.
        """
        monkeypatch.setattr(time, 'sleep', lambda _s: None)

    def test_cursor_mode(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test cursor-based pagination end-to-end via CLI."""
        # pylint: disable=unused-argument

        out_path = tmp_path / 'cursor.json'
        pipeline_yaml = f"""
name: cursor_test
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

        # Mock extract('api', ...) to return cursor-driven pages.
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

        def fake_request(
            self: rmod.RequestManager,
            method: str,
            url: str,
            *,
            session: Any,
            timeout: Any,
            **kwargs: Any,
        ) -> Any:
            assert method == 'GET'
            return fake_extract('api', url, **kwargs)

        # Patch extract targets consistent with the page/offset test.
        monkeypatch.setattr(cli_mod, 'extract', fake_extract)
        monkeypatch.setattr(rmod.RequestManager, 'request_once', fake_request)

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

    def test_cursor_mode_missing_records_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test cursor pagination when ``records_path`` is omitted."""
        # pylint: disable=unused-argument

        # Omits records_path and relies on fallback coalescing behavior.
        out_path = tmp_path / 'cursor_no_records_path.json'
        pipeline_yaml = f"""
name: cursor_test_no_records_path
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
  - name: api_cursor_no_records
    extract:
      source: src
      options:
        pagination:
          type: cursor
          cursor_param: cursor
          cursor_path: next
          page_size: 2
          # records_path intentionally omitted
    load:
      target: dest
"""
        cfg = _write_pipeline(tmp_path, pipeline_yaml)

        def fake_extract(kind: str, _url: str, **kwargs: Any):
            assert kind == 'api'
            params = kwargs.get('params') or {}
            cur = params.get('cursor')
            limit = int(params.get('limit', 2))
            assert limit == 2
            if cur is None:
                return {'items': [{'id': 'x'}, {'id': 'y'}], 'next': 'tok1'}
            if cur == 'tok1':
                return {'items': [{'id': 'z'}], 'next': None}
            return {'items': [], 'next': None}

        def fake_request(
            self: rmod.RequestManager,
            method: str,
            url: str,
            *,
            session: Any,
            timeout: Any,
            **kwargs: Any,
        ) -> Any:
            assert method == 'GET'
            return fake_extract('api', url, **kwargs)

        monkeypatch.setattr(cli_mod, 'extract', fake_extract)
        monkeypatch.setattr(rmod.RequestManager, 'request_once', fake_request)
        monkeypatch.setattr(
            sys,
            'argv',
            [
                'etlplus',
                'pipeline',
                '--config',
                cfg,
                '--run',
                'api_cursor_no_records',
            ],
        )
        rc = main()
        assert rc == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload.get('status') == 'ok'
        data = json.loads(out_path.read_text(encoding='utf-8'))
        assert [r['id'] for r in data] == ['x', 'y', 'z']

    @pytest.mark.parametrize(
        'scenario',
        [
            PageScenario(
                name='page_offset_basic',
                page_size=2,
                pages=[[{'id': 1}, {'id': 2}], [{'id': 3}]],
                expected_ids=[1, 2, 3],
            ),
            PageScenario(
                name='page_offset_trim',
                page_size=3,
                pages=[[{'id': 1}, {'id': 2}, {'id': 3}], [{'id': 4}]],
                expected_ids=[1, 2],
                max_records=2,
            ),
        ],
        ids=lambda s: s.name,
    )
    def test_page_offset_modes(
        self,
        scenario: PageScenario,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test page/offset pagination end-to-end via CLI."""
        # pylint: disable=unused-argument

        # Prepare output path.
        out_path = tmp_path / f'{scenario.name}.json'
        max_records_yaml = (
            f'\n          max_records: {scenario.max_records}'
            if scenario.max_records is not None
            else ''
        )

        # Minimal pipeline with API source using page/offset pagination.
        pipeline_yaml = f"""
name: {scenario.name}
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
  - name: job_{scenario.name}
    extract:
      source: src
      options:
        pagination:
          type: page
          page_param: page
          size_param: per_page
          page_size: {scenario.page_size}{max_records_yaml}
    load:
      target: dest
"""
        cfg = _write_pipeline(tmp_path, pipeline_yaml)

        # Mock extract to return scenario-driven items per page.
        def fake_extract(kind: str, _url: str, **kwargs: Any):
            assert kind == 'api'
            params = kwargs.get('params') or {}
            page = int(params.get('page', 1))
            size = int(params.get('per_page', scenario.page_size))
            assert size == scenario.page_size
            # Pages are 1-indexed; return shorter batch to signal stop.
            if 1 <= page <= len(scenario.pages):
                return scenario.pages[page - 1]
            return []

        def fake_request(
            self: rmod.RequestManager,
            method: str,
            url: str,
            *,
            session: Any,
            timeout: Any,
            **kwargs: Any,
        ) -> Any:
            assert method == 'GET'
            return fake_extract('api', url, **kwargs)

        # Patch extract targets:
        # - cli_mod.extract: CLI may call extract directly for some paths.
        # - RequestManager.request_once: paginate now delegates to the
        #   shared HTTP helper per page.
        monkeypatch.setattr(cli_mod, 'extract', fake_extract)
        monkeypatch.setattr(rmod.RequestManager, 'request_once', fake_request)

        # Run CLI.
        monkeypatch.setattr(
            sys,
            'argv',
            [
                'etlplus',
                'pipeline',
                '--config',
                cfg,
                '--run',
                f'job_{scenario.name}',
            ],
        )
        rc = main()
        assert rc == 0

        payload = json.loads(capsys.readouterr().out)
        assert payload.get('status') == 'ok'

        # Output should contain 3 aggregated items.
        data = json.loads(out_path.read_text(encoding='utf-8'))
        assert [r['id'] for r in data] == scenario.expected_ids

    @pytest.mark.parametrize(
        'scenario',
        [
            {
                'name': 'page_zero_start_coerces_to_one',
                'pagination': {
                    'type': 'page',
                    'page_param': 'page',
                    'size_param': 'per_page',
                    'start_page': 0,
                    'page_size': 10,
                },
                'expect': {'type': 'page', 'start_page': 1, 'page_size': 10},
            },
            {
                'name': 'page_zero_size_coerces_default',
                'pagination': {
                    'type': 'page',
                    'page_param': 'page',
                    'size_param': 'per_page',
                    'start_page': 1,
                    'page_size': 0,
                },
                'expect': {'type': 'page', 'start_page': 1, 'page_size': 100},
            },
            {
                'name': 'cursor_zero_size_coerces_default',
                'pagination': {
                    'type': 'cursor',
                    'cursor_param': 'cursor',
                    'cursor_path': 'next',
                    'page_size': 0,
                },
                'expect': {'type': 'cursor', 'page_size': 100},
            },
            {
                'name': 'limits_pass_through',
                'pagination': {
                    'type': 'page',
                    'page_param': 'page',
                    'size_param': 'per_page',
                    'start_page': 1,
                    'page_size': 5,
                    'max_pages': 2,
                    'max_records': 3,
                },
                'expect': {'type': 'page', 'max_pages': 2, 'max_records': 3},
            },
        ],
        ids=lambda s: s['name'],
    )
    def test_pagination_edge_cases(
        self,
        scenario: dict,
        pipeline_cfg_factory: Callable[..., PipelineConfig],
        fake_endpoint_client: tuple[
            type[FakeEndpointClientProtocol],
            list[FakeEndpointClientProtocol],
        ],
        run_patched: Callable[..., dict[str, Any]],
    ) -> None:  # noqa: D401
        """
        Test edge cases for pagination coalescing using shared fixtures.

        This drives the runner wiring directly (not CLI) to assert the exact
        pagination mapping seen by the client after defaults/overrides.
        """
        cfg = pipeline_cfg_factory()
        job = cfg.jobs[0]
        opts = {}
        if job.extract is not None and hasattr(job.extract, 'options'):
            opts = dict(job.extract.options)
        opts.update({'pagination': scenario['pagination']})
        if job.extract is not None:
            job.extract.options = opts

        fake_client, created = fake_endpoint_client
        result = run_patched(cfg, fake_client)

        assert result.get('status') in {'ok', 'success'}
        assert created, 'Expected client to be constructed'

        seen_pag = created[0].seen.get('pagination')
        assert isinstance(seen_pag, dict)
        for k, v in scenario['expect'].items():
            assert seen_pag.get(k) == v
