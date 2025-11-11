"""
tests.integration.test_run integration tests module.

Integration test for runner target composition.

Exercises `run()` orchestration end-to-end for service + endpoint target URL
composition.
"""
from __future__ import annotations

import importlib

from etlplus.config import ApiConfig
from etlplus.config import ApiProfileConfig
from etlplus.config import ConnectorApi
from etlplus.config import ConnectorFile
from etlplus.config import EndpointConfig
from etlplus.config import ExtractRef
from etlplus.config import JobConfig
from etlplus.config import LoadRef
from etlplus.config import PipelineConfig


# SECTION: HELPERS ========================================================== #


run_mod = importlib.import_module('etlplus.run')


# SECTION: TESTS ============================================================ #


def test_target_service_endpoint_uses_base_path(
    monkeypatch,
    tmp_path,
    capture_load_to_api,
):
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
        apis={'myapi': api},
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
                api='myapi',
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
    import requests

    def _fake_post(url, json=None, timeout=None, **_k):  # noqa: D401
        class R:
            status_code = 200
            text = 'ok'

            def json(self_inner):
                return {'echo': json}

            def raise_for_status(self_inner):
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
