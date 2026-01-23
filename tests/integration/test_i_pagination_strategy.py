"""
:mod:`tests.integration.test_i_pagination_strategy` module.

Integration tests for pagination strategies. We mock API extraction for both
page/offset and cursor modes and drive the CLI entry point to exercise the
public path under real configuration semantics.

Notes
-----
- Pagination logic resides on ``EndpointClient.paginate_url``; patching the
    RequestManager ``request_once`` helper suffices to intercept page fetches.
- Some legacy paths still use module-level extractors; we patch both the
    Typer handlers and :mod:`etlplus.ops.extract` for safety.
- ``time.sleep`` is neutralized to keep tests fast and deterministic.
"""

from __future__ import annotations

import importlib
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

import pytest

import etlplus.api.request_manager as rm_module
import etlplus.cli.handlers as cli_handlers
from etlplus.cli import main
from etlplus.workflow.pipeline import PipelineConfig
from tests.integration.conftest import FakeEndpointClientProtocol

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.integration

extract_module = importlib.import_module('etlplus.ops.extract')


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

    def render_options_block(self) -> str:
        """Render a pagination options block for page-based scenarios."""

        max_records_line = (
            f'\n      max_records: {self.max_records}'
            if self.max_records is not None
            else ''
        )
        return dedent(
            f"""
            pagination:
                type: page
                page_param: page
                size_param: per_page
                page_size: {self.page_size}{max_records_line}
            """,
        )


@dataclass(slots=True)
class CursorBatch:
    """Single cursor pagination batch definition."""

    records: list[dict[str, Any]]
    next_cursor: str | None


@dataclass(slots=True)
class CursorScenario:
    """Cursor pagination scenario definition for CLI integration tests."""

    name: str
    page_size: int
    batches: tuple[CursorBatch, ...]
    expected_ids: list[str]
    options_block: str
    records_key: str = 'data'

    def render_options_block(self) -> str:
        """Return the dedented pagination block for the scenario."""

        return dedent(self.options_block)


CURSOR_SCENARIOS: tuple[CursorScenario, ...] = (
    CursorScenario(
        name='cursor_records_path_explicit',
        page_size=2,
        records_key='data',
        expected_ids=['a', 'b', 'c'],
        options_block="""
        pagination:
            type: cursor
            cursor_param: cursor
            cursor_path: next
            page_size: 2
            records_path: data
        """,
        batches=(
            CursorBatch(
                records=[{'id': 'a'}, {'id': 'b'}],
                next_cursor='tok1',
            ),
            CursorBatch(records=[{'id': 'c'}], next_cursor=None),
        ),
    ),
    CursorScenario(
        name='cursor_records_path_inferred',
        page_size=2,
        records_key='items',
        expected_ids=['x', 'y', 'z'],
        options_block="""
        pagination:
          type: cursor
          cursor_param: cursor
          cursor_path: next
          page_size: 2
          # records_path intentionally omitted
        """,
        batches=(
            CursorBatch(
                records=[{'id': 'x'}, {'id': 'y'}],
                next_cursor='tok1',
            ),
            CursorBatch(records=[{'id': 'z'}], next_cursor=None),
        ),
    ),
)


@dataclass(slots=True)
class PaginationEdgeCase:
    """Edge-case pagination scenario definitions."""

    name: str
    pagination: dict[str, Any]
    expect: dict[str, Any]


def _run_pipeline_and_collect(
    *,
    capsys: pytest.CaptureFixture[str],
    out_path: Path,
    pipeline_cli_runner: Callable[..., str],
    pipeline_yaml: str,
    job_name: str,
    extract_func: Callable[..., Any],
) -> list[dict[str, Any]]:
    """Run the CLI pipeline and return parsed output rows.

    Parameters
    ----------
    capsys : pytest.CaptureFixture[str]
        Pytest capture fixture for CLI STDOUT.
    out_path : Path
        File path where the pipeline writes JSON results.
    pipeline_cli_runner : Callable[..., str]
        Helper that writes the YAML to disk and invokes the CLI.
    pipeline_yaml : str
        YAML configuration to execute.
    job_name : str
        Job name passed to the CLI ``--job`` flag.
    extract_func : Callable[..., Any]
        Fake API extractor used to satisfy HTTP calls.

    Returns
    -------
    list[dict[str, Any]]
        Parsed JSON rows written by the pipeline run.
    """

    pipeline_cli_runner(
        yaml_text=pipeline_yaml,
        run_name=job_name,
        extract_func=extract_func,
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload.get('status') == 'ok'
    return json.loads(out_path.read_text(encoding='utf-8'))


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
    # pylint: disable=unused-argument

    def _run(
        *,
        yaml_text: str,
        run_name: str,
        extract_func: Callable[..., Any],
        request_func: Callable[..., Any] | None = None,
    ) -> str:
        cfg_path = _write_pipeline(tmp_path, yaml_text)
        monkeypatch.setattr(cli_handlers, 'extract', extract_func)
        monkeypatch.setattr(extract_module, 'extract', extract_func)

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
            ['etlplus', 'run', '--config', cfg_path, '--job', run_name],
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

    @pytest.mark.parametrize(
        'scenario',
        CURSOR_SCENARIOS,
        ids=lambda s: s.name,
    )
    def test_cursor_modes(
        self,
        scenario: CursorScenario,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        pipeline_cli_runner: Callable[..., str],
    ) -> None:
        """
        Test cursor-based pagination scenarios end-to-end via the CLI.

        Parameters
        ----------
        scenario : CursorScenario
            Cursor pagination scenario to execute.
        tmp_path : Path
            Temporary directory managed by pytest.
        capsys : pytest.CaptureFixture[str]
            Capture fixture for CLI STDOUT/stderr.
        pipeline_cli_runner : Callable[..., str]
            Helper that materializes and executes the pipeline configuration.
        """

        out_path = tmp_path / f'{scenario.name}.json'
        pipeline_yaml = _build_api_pipeline_yaml(
            name=scenario.name,
            job_name=f'job_{scenario.name}',
            out_path=out_path,
            options_block=scenario.render_options_block(),
        )

        cursor_tracker: dict[str, str | None] = {'expected': None}
        batches = iter(scenario.batches)

        def fake_extract(kind: str, _url: str, **kwargs: Any):
            assert kind == 'api'
            params = kwargs.get('params') or {}
            cur = params.get('cursor')
            limit = int(params.get('limit', scenario.page_size))
            assert cur == cursor_tracker['expected']
            assert limit == scenario.page_size

            try:
                batch = next(batches)
            except StopIteration:
                return {scenario.records_key: [], 'next': None}

            cursor_tracker['expected'] = batch.next_cursor
            return {
                scenario.records_key: batch.records,
                'next': batch.next_cursor,
            }

        data = _run_pipeline_and_collect(
            capsys=capsys,
            out_path=out_path,
            pipeline_cli_runner=pipeline_cli_runner,
            pipeline_yaml=pipeline_yaml,
            job_name=f'job_{scenario.name}',
            extract_func=fake_extract,
        )
        assert [r['id'] for r in data] == scenario.expected_ids

    @pytest.mark.parametrize(
        'scenario',
        (
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
        ),
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
        job_name = f'job_{scenario.name}'

        pipeline_yaml = _build_api_pipeline_yaml(
            name=scenario.name,
            job_name=job_name,
            out_path=out_path,
            options_block=scenario.render_options_block(),
        )

        # Mock extract to return scenario-driven items per page.
        def fake_extract(kind: str, _url: str, **kwargs: Any):
            assert kind == 'api'
            params = kwargs.get('params') or {}
            page = int(params.get('page', 1))
            size = int(params.get('per_page', scenario.page_size))
            # Calculate remaining max_records for this request, if set
            remaining = scenario.max_records
            if remaining is not None:
                # Estimate how many records have already been emitted
                # (page-1)*size is a safe upper bound for this test's structure
                already_emitted = (page - 1) * size
                remaining = max(0, remaining - already_emitted)
                if remaining == 0:
                    return {'items': []}
                size = min(size, remaining)
            # Pages are 1-indexed; return up to 'size' records for this page.
            if 1 <= page <= len(scenario.pages):
                page_records = scenario.pages[page - 1][:size]
                return {'items': page_records}
            return {'items': []}

        data = _run_pipeline_and_collect(
            capsys=capsys,
            out_path=out_path,
            pipeline_cli_runner=pipeline_cli_runner,
            pipeline_yaml=pipeline_yaml,
            job_name=job_name,
            extract_func=fake_extract,
        )
        assert [r['id'] for r in data] == scenario.expected_ids

    EDGE_CASES = (
        PaginationEdgeCase(
            name='page_zero_start_coerces_to_one',
            pagination={
                'type': 'page',
                'page_param': 'page',
                'size_param': 'per_page',
                'start_page': 0,
                'page_size': 10,
            },
            expect={'type': 'page', 'start_page': 1, 'page_size': 10},
        ),
        PaginationEdgeCase(
            name='page_zero_size_coerces_default',
            pagination={
                'type': 'page',
                'page_param': 'page',
                'size_param': 'per_page',
                'start_page': 1,
                'page_size': 0,
            },
            expect={'type': 'page', 'start_page': 1, 'page_size': 100},
        ),
        PaginationEdgeCase(
            name='cursor_zero_size_coerces_default',
            pagination={
                'type': 'cursor',
                'cursor_param': 'cursor',
                'cursor_path': 'next',
                'page_size': 0,
            },
            expect={'type': 'cursor', 'page_size': 100},
        ),
        PaginationEdgeCase(
            name='limits_pass_through',
            pagination={
                'type': 'page',
                'page_param': 'page',
                'size_param': 'per_page',
                'start_page': 1,
                'page_size': 5,
                'max_pages': 2,
                'max_records': 3,
            },
            expect={'type': 'page', 'max_pages': 2, 'max_records': 3},
        ),
    )

    @pytest.mark.parametrize('scenario', EDGE_CASES, ids=lambda s: s.name)
    def test_pagination_edge_cases(
        self,
        scenario: PaginationEdgeCase,
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
            extract_options={'pagination': deepcopy(scenario.pagination)},
        )

        fake_client, created = fake_endpoint_client
        result = run_patched(cfg, fake_client)

        assert result.get('status') in {'ok', 'success'}
        assert created, 'Expected client to be constructed'

        seen_pag = created[0].seen.get('pagination')
        assert isinstance(seen_pag, dict)
        for k, v in scenario.expect.items():
            assert seen_pag.get(k) == v
