"""
:mod:`tests.integration.test_i_run` module.

Validates :func:`run` orchestration end-to-end for service + endpoint URL
composition under a minimal pipeline wiring (file source → API target).

Notes
-----
- Ensures profile ``base_path`` is joined with endpoint path.
- Patches nothing network-related; uses real file source for realism.
- Asserts composed URL and capture of API load invocation via fixture.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any

import pytest
from pytest import MonkeyPatch

from etlplus.workflow import PipelineConfig

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.integration


run_mod = importlib.import_module('etlplus.ops.run')


# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    ('base_path', 'endpoint_path', 'expected_suffix'),
    [
        ('/v1', '/ingest', '/v1/ingest'),
        (None, '/bulk', '/bulk'),
    ],
    ids=['with-base-path', 'without-base-path'],
)
def test_target_service_endpoint_uses_base_path(
    monkeypatch: MonkeyPatch,
    capture_load_to_api: dict[str, Any],
    file_to_api_pipeline_factory: Callable[..., PipelineConfig],
    base_url: str,
    base_path: str | None,
    endpoint_path: str,
    expected_suffix: str,
):
    """Test composed API URLs across optional base_path configurations."""

    cfg = file_to_api_pipeline_factory(
        base_path=base_path,
        endpoint_path=endpoint_path,
        headers={'Content-Type': 'application/json'},
    )
    monkeypatch.setattr(run_mod, 'load_pipeline_config', lambda *_a, **_k: cfg)

    result = run_mod.run('send')

    assert result.get('status') in {'ok', 'success'}
    assert capture_load_to_api['url'] == f'{base_url}{expected_suffix}'

    headers = capture_load_to_api.get('headers') or {}
    assert headers.get('Content-Type') == 'application/json'
