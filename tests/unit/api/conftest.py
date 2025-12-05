"""
:mod:`tests.unit.api.conftest` module.

Configures pytest-based unit tests for and provides shared fixtures for
:mod:`etlplus.api`.

Notes
-----
- Fixtures are designed for reuse and DRY test setup across API-focused
unit tests.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

import etlplus.api.request as request_mod
from etlplus.api import EndpointClient

# SECTION: FIXTURES ========================================================= #


@pytest.fixture
def client() -> EndpointClient:
    """
    Construct an :class:`EndpointClient` with retry enabled.

    Returns
    -------
    EndpointClient
        Client instance pointing at a dummy base URL and endpoint map.
    """
    return EndpointClient(
        base_url='https://api.example.com',
        base_path='v1',
        endpoints={'dummy': '/dummy'},
        retry_network_errors=True,
    )


@pytest.fixture
def rest_client_custom(
    request: pytest.FixtureRequest,
) -> EndpointClient:
    """
    Parameterized EndpointClient fixture for custom config.

    Parameters
    ----------
    request : pytest.FixtureRequest
        Pytest request fixture for accessing parameterization.

    Returns
    -------
    EndpointClient
        Configured EndpointClient instance.
    """
    params = getattr(request, 'param', None) or {}

    return EndpointClient(**params)


@pytest.fixture
def rest_client_default() -> EndpointClient:
    """
    Default EndpointClient with no endpoints.

    Returns
    -------
    EndpointClient
        Configured EndpointClient instance.
    """
    return EndpointClient(
        base_url='https://api.example.com',
        endpoints={},
    )


@pytest.fixture
def rest_client_with_endpoints() -> EndpointClient:
    """
    EndpointClient with sample endpoints for API tests.

    Returns
    -------
    EndpointClient
        Configured EndpointClient instance.
    """
    return EndpointClient(
        base_url='https://api.example.com',
        base_path='v1',
        endpoints={'list': '/items', 'x': '/x'},
    )


@pytest.fixture(autouse=True)
def patch_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Disable sleeping during tests to keep the suite fast.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Built-in pytest fixture used to patch attributes.
    """
    monkeypatch.setattr(
        EndpointClient,
        'apply_sleep',
        staticmethod(lambda _: None),
    )


# Additional fixtures for retry/jitter testing wired to RetryManager.sleeper.

@pytest.fixture
def capture_sleeps(
    monkeypatch: pytest.MonkeyPatch,
) -> list[float]:
    """
    Capture sleep durations from retries and rate limiting.

    Patches :class:`RetryManager` so that its ``sleeper`` callable appends
    sleep durations to a list instead of actually sleeping. Also patches
    :meth:`EndpointClient.apply_sleep` to record rate-limit sleeps into
    the same list.
    """
    sleeps: list[float] = []

    # Patch RetryManager to inject a recording sleeper when none is given.
    original_init = request_mod.RetryManager.__init__

    def _init(self, *args, **kwargs):
        if 'sleeper' not in kwargs:
            def _sleeper(seconds: float) -> None:
                sleeps.append(seconds)
            kwargs['sleeper'] = _sleeper
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(
        request_mod.RetryManager,
        '__init__',
        _init,  # type: ignore[assignment]
    )

    # Patch EndpointClient.apply_sleep so rate-limit sleeps are captured.
    def _capture_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(
        EndpointClient,
        'apply_sleep',
        staticmethod(_capture_sleep),
    )

    return sleeps


@pytest.fixture
def jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[list[float]], list[float]]:
    """
    Configure deterministic jitter values for retry backoff.

    Returns a callable that, when invoked with a list of floats, seeds the
    sequence of values returned by :func:`random.uniform`.
    """
    values: list[float] = []

    def set_values(new_values: list[float]) -> list[float]:
        values.clear()
        values.extend(new_values)
        return values

    def fake_uniform(_a: float, b: float) -> float:
        if values:
            return values.pop(0)
        return b

    monkeypatch.setattr(
        request_mod.random,
        'uniform',
        fake_uniform,
    )
    return set_values
