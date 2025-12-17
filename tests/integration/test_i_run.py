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
from pathlib import Path
from typing import Any

from pytest import MonkeyPatch

from etlplus.api import ApiConfig
from etlplus.api import ApiProfileConfig
from etlplus.api import EndpointConfig
from etlplus.config import ConnectorApi
from etlplus.config import ConnectorFile
from etlplus.config import ExtractRef
from etlplus.config import JobConfig
from etlplus.config import LoadRef
from etlplus.config import PipelineConfig

# SECTION: HELPERS ========================================================== #


run_mod = importlib.import_module('etlplus.run')


# SECTION: TESTS ============================================================ #


def test_target_service_endpoint_uses_base_path(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    capture_load_to_api: dict[str, Any],
):
    """Test that API target URL composes profile base_path + endpoint path."""
    # pylint: disable=unused-argument

    # Make a simple source file so extract step succeeds without mocks.
    src_path = tmp_path / 'data.json'
    src_path.write_text('{"ok": true}\n', encoding='utf-8')

    # API config with base_path via profile.
    api = ApiConfig(
        base_url='https://api.example.com',
        profiles={
            'default': ApiProfileConfig(
                base_url='https://api.example.com',
                headers={},
                base_path='/v1',
            ),
        },
        endpoints={'ingest': EndpointConfig(path='/ingest')},
    )

    # Pipeline wiring: file source -> api target (service + endpoint).
    cfg = PipelineConfig(
        apis={'my_api': api},
        sources=[
            ConnectorFile(
                name='local_json',
                type='file',
                format='json',
                path=str(src_path),
            ),
        ],
        targets=[
            ConnectorApi(
                name='ingest_out',
                type='api',
                api='my_api',
                endpoint='ingest',
                method='post',
                headers={'Content-Type': 'application/json'},
            ),
        ],
        jobs=[
            JobConfig(
                name='send',
                extract=ExtractRef(source='local_json'),
                load=LoadRef(target='ingest_out'),
            ),
        ],
    )

    # Patch the config loader to return our in-memory config.
    monkeypatch.setattr(run_mod, 'load_pipeline_config', lambda *_a, **_k: cfg)

    # Stub network POST to avoid real DNS / HTTP.
    import requests  # type: ignore[import]

    def _fake_post(url, json=None, timeout=None, **_k):
        """Return a fake HTTP response object for POST calls."""

        class R:
            """Lightweight fake response object used for testing."""

            status_code = 200
            text = 'ok'

            def json(self):
                """Return JSON data."""
                return {'echo': json}

            def raise_for_status(self):
                """Raise nothing for HTTP 200 OK status."""
                return None

        return R()

    monkeypatch.setattr(requests, 'post', _fake_post)

    result = run_mod.run('send')

    assert result.get('status') in {'ok', 'success'}
    assert capture_load_to_api['url'] == 'https://api.example.com/v1/ingest'

    # Ensure headers merged include Content-Type from target.
    assert isinstance(capture_load_to_api['headers'], dict)
    assert (
        capture_load_to_api['headers'].get('Content-Type')
        == 'application/json'
    )
