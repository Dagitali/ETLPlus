"""
Runner target composition: service + endpoint
============================================

Verify that when a job targets an API using service + endpoint, the runner
composes the URL with the API's base_path (via effective_base_url) and calls
load_to_api with that URL.
"""
from __future__ import annotations

import importlib
from typing import Any

from etlplus.config import ApiConfig
from etlplus.config import ApiProfileConfig
from etlplus.config import EndpointConfig
from etlplus.config import ExtractRef
from etlplus.config import JobConfig
from etlplus.config import LoadRef
from etlplus.config import PipelineConfig
from etlplus.config import SourceFile
from etlplus.config import TargetApi

run_mod = importlib.import_module('etlplus.run')
load_mod = importlib.import_module('etlplus.load')


def test_target_service_endpoint_uses_base_path(monkeypatch, tmp_path):
    # Make a simple source file so extract step succeeds without mocks
    src_path = tmp_path / 'data.json'
    src_path.write_text('{"ok": true}\n', encoding='utf-8')

    # API config with base_path via profile
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

    # Pipeline wiring: file source -> api target (service + endpoint)
    cfg = PipelineConfig(
        apis={'myapi': api},
        sources=[
            SourceFile(
                name='local_json',
                type='file',
                format='json',
                path=str(src_path),
            ),
        ],
        targets=[
            TargetApi(
                name='ingest_out', type='api', api='myapi', endpoint='ingest',
                method='post', headers={'Content-Type': 'application/json'},
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

    # Patch the config loader to return our in-memory config
    monkeypatch.setattr(run_mod, 'load_pipeline_config', lambda *_a, **_k: cfg)

    # Capture the final URL passed to load_to_api
    seen: dict[str, Any] = {}

    def fake_load_to_api(_data: Any, url: str, method: str, **kwargs: Any):
        seen['url'] = url
        seen['method'] = method
        seen['headers'] = kwargs.get('headers')
        return {'status': 'ok', 'url': url}

    monkeypatch.setattr(load_mod, 'load_to_api', fake_load_to_api)

    result = run_mod.run('send')

    assert result.get('status') == 'ok'
    assert seen['url'] == 'https://api.example.com/v1/ingest'
    # Ensure headers merged include Content-Type from target
    assert isinstance(seen['headers'], dict)
    assert seen['headers'].get('Content-Type') == 'application/json'
