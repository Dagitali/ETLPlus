"""
tests.integration.conftest pytest configuration module.

Configures pytest-based integration tests and provides shared fixtures to
reduce duplication across integration tests.
"""
from __future__ import annotations

import importlib
import pathlib
from typing import Any
from typing import Callable

import pytest

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
from etlplus.config import RateLimitConfig


# SECTION: HELPERS ========================================================== #


# Mark all tests in this directory as integration tests.
pytestmark = pytest.mark.integration


# SECTION: FIXTURES ========================================================= #


@pytest.fixture
def fake_endpoint_client() -> tuple[type, list[object]]:  # noqa: ANN201
    """
    Provide a Fake EndpointClient class and capture list.

    The returned class records 'pagination' and 'sleep_seconds' seen by
    its paginate() method. The second element of the tuple is the list of
    created instances for assertions.

    Returns
    -------
    tuple[type, list[object]]
        A tuple where the first element is the FakeClient class and the
        second element is the list of created instances.
    """
    created: list[object] = []

    class FakeClient:
        def __init__(
            self, base_url: str, endpoints: dict[str, str], **_k: Any,
        ):
            self.base_url = base_url
            self.endpoints = endpoints
            self.seen: dict[str, Any] = {}
            created.append(self)

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
            self.seen['sleep_seconds'] = _sleep_seconds
            return [{'ok': True}]

    return FakeClient, created


@pytest.fixture
def pipeline_cfg_factory(
    tmp_path: pathlib.Path,  # pytest tmp_path fixture
) -> Callable[..., PipelineConfig]:
    """
    Factory to build a minimal PipelineConfig for runner tests.

    Accepts optional pagination and rate limit defaults at the API profile
    level. Creates a single job named 'job' with a source that references
    the API endpoint 'items' and a file target.

    Parameters
    ----------
    tmp_path : pathlib.Path
        The pytest temporary path fixture.

    Returns
    -------
    Callable[..., PipelineConfig]
        A factory function to create PipelineConfig instances.
    """

    def _make(
        *,
        pagination_defaults: PaginationConfig | None = None,
        rate_limit_defaults: RateLimitConfig | None = None,
    ) -> PipelineConfig:
        prof = ApiProfileConfig(
            base_url='https://api.example.com',
            headers={},
            base_path='/v1',
            auth={},
            rate_limit_defaults=rate_limit_defaults,
            pagination_defaults=pagination_defaults,
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
            name='t', type='file', format='json', path=str(out_path),
        )
        return PipelineConfig(
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

    return _make


@pytest.fixture
def run_patched(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[..., dict[str, Any]]:
    """
    Return a helper to run the pipeline with patched runner dependencies.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        The pytest monkeypatch fixture.

    Returns
    -------
    Callable[..., dict[str, Any]]
        A factory function to run the pipeline with patched dependencies.

    Example
    -------
    result = run_patched(cfg, FakeClient, sleep_seconds=1.23)
    """

    run_mod = importlib.import_module('etlplus.run')

    def _run(
        cfg: PipelineConfig,
        endpoint_client_cls: type,
        *,
        sleep_seconds: float | None = None,
    ) -> dict[str, Any]:
        # Patch config loader and EndpointClient
        monkeypatch.setattr(
            run_mod, 'load_pipeline_config', lambda *_a, **_k: cfg,
        )
        monkeypatch.setattr(run_mod, 'EndpointClient', endpoint_client_cls)

        # Optionally force compute_sleep_seconds to a constant
        if sleep_seconds is not None:
            monkeypatch.setattr(
                run_mod,
                'compute_sleep_seconds',
                lambda *_a, **_k: sleep_seconds,
            )

        # Avoid real IO in load()
        def _fake_load(data: Any, *_a: Any, **_k: Any) -> dict[str, Any]:
            n = len(data) if isinstance(data, list) else 0
            return {'status': 'ok', 'count': n}

        monkeypatch.setattr(run_mod, 'load', _fake_load)

        return run_mod.run('job')

    return _run
