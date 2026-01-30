"""
:mod:`tests.integration.conftest` module.

Configures pytest-based integration tests and provides shared fixtures.

Notes
-----
- Fixtures are designed for reuse and DRY test setup.
"""

from __future__ import annotations

import importlib
import json
import pathlib
from collections.abc import Callable
from typing import Any
from typing import Protocol

import pytest

from etlplus.api import ApiConfig
from etlplus.api import ApiProfileConfig
from etlplus.api import EndpointConfig
from etlplus.api import PaginationConfig
from etlplus.api import RateLimitConfig
from etlplus.api import RateLimiter
from etlplus.connector import ConnectorApi
from etlplus.connector import ConnectorFile
from etlplus.workflow import ExtractRef
from etlplus.workflow import JobConfig
from etlplus.workflow import LoadRef
from etlplus.workflow import PipelineConfig

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.integration


# SECTION: HELPERS ========================================================== #


# Directory-level marker for integration tests.
pytestmark = pytest.mark.integration


# Protocol describing the fake endpoint client shape for type checking.
class FakeEndpointClientProtocol(Protocol):
    """
    Protocol for fake endpoint clients used in integration tests.

    Attributes
    ----------
    seen : dict[str, Any]
        Dictionary capturing values observed during pagination.
    """

    seen: dict[str, Any]


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='capture_load_to_api')
def capture_load_to_api_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    """
    Capture API load calls by patching the high-level ``load`` wrapper.

    We monkeypatch ``etlplus.ops.load.load`` because the runner invokes
    ``load(data, 'api', url, method=...)`` rather than calling
    ``load_to_api`` directly. The stub detects API loads and records
    request details for assertions.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture for patching functions.

    Returns
    -------
    dict[str, Any]
        Mutable dictionary populated after a run where API load happens.
    """
    # Ensure module, not attr.
    _load_mod = importlib.import_module('etlplus.ops.load')

    seen: dict[str, Any] = {}

    def _fake_load_to_api_env(
        data: Any,
        env: dict[str, Any],
    ) -> dict[str, Any]:
        seen['url'] = env.get('url')
        seen['method'] = env.get('method')
        seen['headers'] = env.get('headers')
        seen['timeout'] = env.get('timeout')
        seen['session'] = env.get('session')

        # Return a minimal success envelope similar to real load_to_api.
        return {
            'status': 'ok',
            'url': env.get('url'),
            'count': len(data) if isinstance(data, list) else 0,
        }

    monkeypatch.setattr(_load_mod, '_load_to_api_env', _fake_load_to_api_env)

    return seen


@pytest.fixture(name='fake_endpoint_client')
def fake_endpoint_client_fixture() -> tuple[
    type[FakeEndpointClientProtocol],
    list[FakeEndpointClientProtocol],
]:  # noqa: ANN201
    """
    Provide a Fake EndpointClient class and capture list.

    The returned class records 'pagination' and 'sleep_seconds' seen by
    its paginate() method. The second element of the tuple is the list of
    created instances for assertions.

    Returns
    -------
    tuple[type[FakeEndpointClientProtocol], list[FakeEndpointClientProtocol]]
        A tuple where the first element is the FakeClient class and the
        second element is the list of created instances.
    """
    created: list[FakeEndpointClientProtocol] = []

    class FakeClient:
        """
        Fake :class:`EndpointClient` capturing :meth:`paginate` arguments.
        """

        # pylint: disable=unused-argument

        seen: dict[str, Any]

        def __init__(
            self,
            base_url: str,
            endpoints: dict[str, str],
            *args,
            **kwargs,
        ):
            self.base_url = base_url
            self.endpoints = endpoints
            self.seen = {}
            created.append(self)

        def paginate(
            self,
            *args,
            pagination: Any | None = None,
            sleep_seconds: float = 0.0,
            **kwargs,
        ) -> Any:
            """
            Capture pagination config and sleep seconds.
            """
            self.seen['pagination'] = pagination
            self.seen['sleep_seconds'] = sleep_seconds
            return [{'ok': True}]

    return FakeClient, created


@pytest.fixture(name='file_to_api_pipeline_factory')
def file_to_api_pipeline_factory_fixture(
    tmp_path: pathlib.Path,
    base_url: str,
) -> Callable[..., PipelineConfig]:
    """Build a pipeline wiring a JSON file source to an API target."""

    def _make(
        *,
        payload: Any | None = None,
        base_url: str = base_url,
        base_path: str | None = '/v1',
        endpoint_path: str = '/ingest',
        endpoint_name: str = 'ingest',
        method: str = 'post',
        headers: dict[str, str] | None = None,
        job_name: str = 'send',
        target_name: str = 'ingest_out',
    ) -> PipelineConfig:
        source_path = tmp_path / f'{job_name}_input.json'
        effective_payload = payload if payload is not None else {'ok': True}
        text = (
            effective_payload
            if isinstance(effective_payload, str)
            else json.dumps(effective_payload)
        )
        source_path.write_text(text, encoding='utf-8')

        profile = ApiProfileConfig(
            base_url=base_url,
            headers={},
            base_path=base_path or '',
            auth={},
            rate_limit_defaults=None,
            pagination_defaults=None,
        )
        api = ApiConfig(
            base_url=base_url,
            profiles={'default': profile},
            endpoints={endpoint_name: EndpointConfig(path=endpoint_path)},
        )

        src = ConnectorFile(
            name='file_src',
            type='file',
            format='json',
            path=str(source_path),
        )
        tgt = ConnectorApi(
            name=target_name,
            type='api',
            api='svc',
            endpoint=endpoint_name,
            method=method,
            headers=headers or {},
        )

        return PipelineConfig(
            apis={'svc': api},
            sources=[src],
            targets=[tgt],
            jobs=[
                JobConfig(
                    name=job_name,
                    extract=ExtractRef(source='file_src'),
                    load=LoadRef(target=target_name),
                ),
            ],
        )

    return _make


@pytest.fixture(name='pipeline_cfg_factory')
def pipeline_cfg_factory_fixture(
    tmp_path: pathlib.Path,
    base_url: str,
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
    base_url : str
        Common base URL used across tests.

    Returns
    -------
    Callable[..., PipelineConfig]
        Factory function to create :class:`PipelineConfig` instances.
    """

    def _make(
        *,
        pagination_defaults: PaginationConfig | None = None,
        rate_limit_defaults: RateLimitConfig | None = None,
        extract_options: dict[str, Any] | None = None,
    ) -> PipelineConfig:
        prof = ApiProfileConfig(
            base_url=base_url,
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
            name='t',
            type='file',
            format='json',
            path=str(out_path),
        )
        job = JobConfig(
            name='job',
            extract=ExtractRef(source='s'),
            load=LoadRef(target='t'),
        )
        if extract_options is not None:
            if job.extract is None:
                msg = 'job.extract is None; cannot set options'
                raise ValueError(msg)
            job.extract.options = extract_options

        return PipelineConfig(
            apis={'svc': api},
            sources=[src],
            targets=[tgt],
            jobs=[job],
        )

    return _make


@pytest.fixture(name='run_patched')
def run_patched_fixture(
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
        Factory function to run the pipeline with patched dependencies.

    Example
    -------
    result = run_patched(cfg, FakeClient, sleep_seconds=1.23)
    """
    run_mod = importlib.import_module('etlplus.ops.run')
    extract_mod = importlib.import_module('etlplus.ops.extract')

    def _run(
        cfg: PipelineConfig,
        endpoint_client_cls: type,
        *,
        sleep_seconds: float | None = None,
    ) -> dict[str, Any]:
        # pylint: disable=unused-argument

        # Patch config loader and EndpointClient.
        monkeypatch.setattr(
            run_mod,
            'load_pipeline_config',
            lambda *_a, **_k: cfg,
        )
        monkeypatch.setattr(
            extract_mod,
            'EndpointClient',
            endpoint_client_cls,
        )

        # Optionally force the resolved rate-limit delay to a constant.
        if sleep_seconds is not None:

            def _fixed_resolve(
                cls: type[RateLimiter],
                *,
                rate_limit: Any | None = None,
                overrides: Any | None = None,
            ) -> float:
                # ignore inputs and return deterministic delay for assertions
                return sleep_seconds

            monkeypatch.setattr(
                RateLimiter,
                'resolve_sleep_seconds',
                classmethod(_fixed_resolve),
            )

        # Avoid real IO in load().
        def _fake_load(
            data: Any,
            *args,
            **kwargs,
        ) -> dict[str, Any]:
            n = len(data) if isinstance(data, list) else 0
            return {'status': 'ok', 'count': n}

        monkeypatch.setattr(run_mod, 'load', _fake_load)

        return run_mod.run('job')

    return _run
