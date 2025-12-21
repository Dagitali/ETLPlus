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
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent
from textwrap import indent
from typing import Any

import pytest  # pylint: disable=unused-import

import etlplus.api.request_manager as rm_module
import etlplus.cli as cli_module
from etlplus.cli import main
from etlplus.config.pipeline import PipelineConfig
from tests.integration.conftest import FakeEndpointClientProtocol

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.integration


def _build_api_pipeline_yaml(
    *,
    name: str,
    job_name: str,
    out_path: Path,
    options_block: str,
    source_url: str = 'https://example.test/api',
) -> str:
    """Render a minimal pipeline YAML for a single API source job."""

    cleaned_block = dedent(options_block).strip()
    if not cleaned_block:
        msg = 'options_block must not be empty'
        raise ValueError(msg)

    indented_options = indent(cleaned_block, ' ' * 8)

    return (
        f"""
name: {name}
sources:
  - name: src
    type: api
    url: {source_url}
targets:
  - name: dest
    type: file
    format: json
    path: {out_path}
jobs:
  - name: {job_name}
    extract:
      source: src
      options:
{indented_options}
    load:
      target: dest
"""
    ).strip()


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


def _run_pipeline_and_collect(
    *,
    capsys: pytest.CaptureFixture[str],
    out_path: Path,
    pipeline_cli_runner: Callable[..., str],
    pipeline_yaml: str,
    run_name: str,
    extract_func: Callable[..., Any],
) -> list[dict[str, Any]]:
    """Run the CLI pipeline and return parsed output rows.

    Parameters
    ----------
    capsys : pytest.CaptureFixture[str]
        Pytest capture fixture for CLI stdout.
    out_path : Path
        File path where the pipeline writes JSON results.
    pipeline_cli_runner : Callable[..., str]
        Helper that writes the YAML to disk and invokes the CLI.
    pipeline_yaml : str
        YAML configuration to execute.
    run_name : str
        Job name passed to the CLI ``--run`` flag.
    extract_func : Callable[..., Any]
        Fake API extractor used to satisfy HTTP calls.

    Returns
    -------
    list[dict[str, Any]]
        Parsed JSON rows written by the pipeline run.
    """

    pipeline_cli_runner(
        yaml_text=pipeline_yaml,
        run_name=run_name,
        extract_func=extract_func,
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload.get('status') == 'ok'
    return json.loads(out_path.read_text(encoding='utf-8'))


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='pipeline_cli_runner')
def pipeline_cli_runner_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[..., str]:
    """
    Provide a helper that runs the CLI against a temporary pipeline.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory provided by pytest.
    monkeypatch : pytest.MonkeyPatch
        Fixture used to patch CLI dependencies.

    Returns
    -------
    Callable[..., str]
        Runner that writes the pipeline YAML, patches HTTP helpers, and
        returns the resulting config path.
    """

    def _run(
        *,
        yaml_text: str,
        run_name: str,
        extract_func: Callable[..., Any],
        request_func: Callable[..., Any] | None = None,
    ) -> str:
        cfg_path = _write_pipeline(tmp_path, yaml_text)
        monkeypatch.setattr(cli_module, 'extract', extract_func)

        def _default_request(
            self: rm_module.RequestManager,
            method: str,
            url: str,
            *,
            session: Any,
            timeout: Any,
            **kwargs: Any,
        ) -> Any:
            return extract_func('api', url, **kwargs)

        monkeypatch.setattr(
            rm_module.RequestManager,
            'request_once',
            request_func or _default_request,
        )
        monkeypatch.setattr(
            sys,
            'argv',
            ['etlplus', 'pipeline', '--config', cfg_path, '--run', run_name],
        )
        rc = main()
        assert rc == 0
        return cfg_path

    return _run


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
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        pipeline_cli_runner: Callable[..., str],
    ) -> None:
        """Test cursor-based pagination end-to-end via CLI."""

        out_path = tmp_path / 'cursor.json'
        pipeline_yaml = _build_api_pipeline_yaml(
            name='cursor_test',
            job_name='api_cursor',
            out_path=out_path,
            options_block="""
            pagination:
              type: cursor
              cursor_param: cursor
              cursor_path: next
              page_size: 2
              records_path: data
            """,
        )

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

        data = _run_pipeline_and_collect(
            capsys=capsys,
            out_path=out_path,
            pipeline_cli_runner=pipeline_cli_runner,
            pipeline_yaml=pipeline_yaml,
            run_name='api_cursor',
            extract_func=fake_extract,
        )
        assert [r['id'] for r in data] == ['a', 'b', 'c']

    def test_cursor_mode_missing_records_path(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        pipeline_cli_runner: Callable[..., str],
    ) -> None:
        """Test cursor pagination when ``records_path`` is omitted."""
        # Omits records_path and relies on fallback coalescing behavior.
        out_path = tmp_path / 'cursor_no_records_path.json'
        pipeline_yaml = _build_api_pipeline_yaml(
            name='cursor_test_no_records_path',
            job_name='api_cursor_no_records',
            out_path=out_path,
            options_block="""
            pagination:
              type: cursor
              cursor_param: cursor
              cursor_path: next
              page_size: 2
              # records_path intentionally omitted
            """,
        )

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

        data = _run_pipeline_and_collect(
            capsys=capsys,
            out_path=out_path,
            pipeline_cli_runner=pipeline_cli_runner,
            pipeline_yaml=pipeline_yaml,
            run_name='api_cursor_no_records',
            extract_func=fake_extract,
        )
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
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        pipeline_cli_runner: Callable[..., str],
    ) -> None:
        """Test page/offset pagination end-to-end via CLI."""
        out_path = tmp_path / f'{scenario.name}.json'
        max_records_line = (
            f'\n  max_records: {scenario.max_records}'
            if scenario.max_records is not None
            else ''
        )
        job_name = f'job_{scenario.name}'

        pipeline_yaml = _build_api_pipeline_yaml(
            name=scenario.name,
            job_name=job_name,
            out_path=out_path,
            options_block=f"""
            pagination:
              type: page
              page_param: page
              size_param: per_page
              page_size: {scenario.page_size}{max_records_line}
            """,
        )
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

        data = _run_pipeline_and_collect(
            capsys=capsys,
            out_path=out_path,
            pipeline_cli_runner=pipeline_cli_runner,
            pipeline_yaml=pipeline_yaml,
            run_name=job_name,
            extract_func=fake_extract,
        )
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
        cfg = pipeline_cfg_factory(
            extract_options={'pagination': deepcopy(scenario['pagination'])},
        )

        fake_client, created = fake_endpoint_client
        result = run_patched(cfg, fake_client)

        assert result.get('status') in {'ok', 'success'}
        assert created, 'Expected client to be constructed'

        seen_pag = created[0].seen.get('pagination')
        assert isinstance(seen_pag, dict)
        for k, v in scenario['expect'].items():
            assert seen_pag.get(k) == v
