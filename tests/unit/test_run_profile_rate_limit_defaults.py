"""
Runner uses profile-level rate_limit defaults
===========================================

Verify that when a source references an API endpoint and neither the source
nor endpoint define rate_limit, the runner inherits rate limit defaults from
the API profile and passes the computed sleep_seconds to the client.
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
from etlplus.config import PipelineConfig
from etlplus.config import RateLimitConfig


run_mod = importlib.import_module('etlplus.run')


def test_profile_rate_limit_defaults_applied(monkeypatch, tmp_path):
    # Assemble an API with profile-level rate_limit defaults only
    prof = ApiProfileConfig(
        base_url='https://api.example.com',
        headers={},
        base_path='/v1',
        auth={},
        rate_limit_defaults=RateLimitConfig(
            sleep_seconds=0.5,
            max_per_sec=None,
        ),
    )
    api = ApiConfig(
        base_url=prof.base_url,
        headers=prof.headers,
        profiles={'default': prof},
        # No endpoint.rate_limit
        endpoints={'items': EndpointConfig(path='/items')},
    )

    # Source references the API + endpoint (no source.rate_limit)
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

    # Capture the sleep_seconds argument that the client receives
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
            self.seen['sleep_seconds'] = _sleep_seconds
            self.seen['pagination'] = pagination
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

    # With only profile rate_limit defaults (sleep_seconds=0.5),
    # the computed sleep_seconds should reach the client.
    seen_sleep = created_clients[0].seen.get('sleep_seconds')
    assert seen_sleep == 0.5


def test_profile_rate_limit_defaults_max_per_sec_applied(
    monkeypatch, tmp_path,
):
    # Profile-level defaults specify only max_per_sec; the runner should call
    # compute_sleep_seconds and forward its return value to the client.
    prof = ApiProfileConfig(
        base_url='https://api.example.com',
        headers={},
        base_path='/v1',
        auth={},
        rate_limit_defaults=RateLimitConfig(
            sleep_seconds=None,
            max_per_sec=4,
        ),
    )
    api = ApiConfig(
        base_url=prof.base_url,
        headers=prof.headers,
        profiles={'default': prof},
        endpoints={'items': EndpointConfig(path='/items')},
    )

    src = ConnectorApi(name='s', type='api', api='svc', endpoint='items')
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
            self.seen['sleep_seconds'] = _sleep_seconds
            self.seen['pagination'] = pagination
            return [{'ok': True}]

    # Stub config loader and EndpointClient
    monkeypatch.setattr(run_mod, 'load_pipeline_config', lambda *_a, **_k: cfg)
    monkeypatch.setattr(run_mod, 'EndpointClient', FakeClient)

    # Stub compute_sleep_seconds to return a sentinel value and verify inputs
    def fake_compute_sleep_seconds(_rl: Any, _ov: Any) -> float:
        return 1.23

    monkeypatch.setattr(
        run_mod, 'compute_sleep_seconds', fake_compute_sleep_seconds,
    )

    # Stub file load
    def fake_load(data: Any, _target_type: str, _path: str, **_k: Any) -> Any:
        count = len(data) if isinstance(data, list) else 0
        return {'status': 'ok', 'count': count}

    monkeypatch.setattr(run_mod, 'load', fake_load)

    result = run_mod.run('job')

    assert result.get('status') == 'ok'
    assert created_clients, 'Expected client to be constructed'
    seen_sleep = created_clients[0].seen.get('sleep_seconds')
    assert seen_sleep == 1.23
