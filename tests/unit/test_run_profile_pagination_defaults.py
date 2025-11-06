"""
Runner uses profile-level pagination defaults
===========================================

Verify that when a source references an API endpoint and neither the source
nor endpoint define pagination, the runner inherits pagination defaults from
the API profile.
"""
from __future__ import annotations

import importlib
from typing import Any

from etlplus.config import ApiConfig
from etlplus.config import ApiProfileConfig
from etlplus.config import ConnectorApi
from etlplus.config import ConnectorFile
from etlplus.config import EndpointConfig
from etlplus.config import ExtractRef
from etlplus.config import JobConfig
from etlplus.config import LoadRef
from etlplus.config import PaginationConfig
from etlplus.config import PipelineConfig


run_mod = importlib.import_module('etlplus.run')


def test_profile_pagination_defaults_applied(monkeypatch, tmp_path):
    # Assemble an API with profile-level pagination defaults only
    prof = ApiProfileConfig(
        base_url='https://api.example.com',
        headers={},
        base_path='/v1',
        auth={},
        pagination_defaults=PaginationConfig(
            type='page',
            page_param='page',
            size_param='per_page',
            start_page=5,
            page_size=50,
        ),
    )
    api = ApiConfig(
        base_url=prof.base_url,
        headers=prof.headers,
        profiles={'default': prof},
        # No endpoint.pagination
        endpoints={'items': EndpointConfig(path='/items')},
    )

    # Source references the API + endpoint (no source.pagination)
    src = ConnectorApi(name='s', type='api', api='svc', endpoint='items')

    # Provide a dummy file target; we'll stub load() to avoid IO
    out_path = tmp_path / 'out.json'
    tgt = ConnectorFile(
        name='t', type='file',
        format='json', path=str(out_path),
    )

    cfg = PipelineConfig(
        apis={'svc': api},
        sources=[src],
        targets=[tgt],
        jobs=[
            JobConfig(
                name='job',
                extract=ExtractRef(source='s'),
                load=LoadRef(target='t'),
            ),
        ],
    )

    # Capture the pagination argument that the client receives
    created_clients: list[Any] = []

    class FakeClient:
        def __init__(
            self, base_url: str, endpoints: dict[str, str], **_k: Any,
        ) -> None:
            self.base_url = base_url
            self.endpoints = endpoints
            self.seen: dict[str, Any] = {}
            created_clients.append(self)

        def paginate(
            self,
            _endpoint_key: str,
            *,
            _params: dict[str, Any] | None = None,
            _headers: dict[str, str] | None = None,
            _timeout: float | None = None,
            pagination: Any | None = None,
            _sleep_seconds: float | None = None,
        ) -> Any:
            self.seen['pagination'] = pagination
            # Return some data; runner will forward to load()
            return [{'ok': True}]

    # Stub the config loader and the client class
    monkeypatch.setattr(run_mod, 'load_pipeline_config', lambda *_a, **_k: cfg)
    monkeypatch.setattr(run_mod, 'EndpointClient', FakeClient)

    # Stub file load to avoid disk writes
    def fake_load(data: Any, _target_type: str, _path: str, **_k: Any) -> Any:
        count = len(data) if isinstance(data, list) else 0
        return {'status': 'ok', 'count': count}

    monkeypatch.setattr(run_mod, 'load', fake_load)

    result = run_mod.run('job')

    # Sanity
    assert result.get('status') == 'ok'
    assert created_clients, 'Expected client to be constructed'

    # Assert the pagination dict came from the profile defaults
    seen_pag = created_clients[0].seen.get('pagination')
    assert isinstance(seen_pag, dict)
    assert seen_pag.get('type') == 'page'
    assert seen_pag.get('page_param') == 'page'
    assert seen_pag.get('size_param') == 'per_page'
    assert seen_pag.get('start_page') == 5
    assert seen_pag.get('page_size') == 50
