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

import etlplus.api.rate_limiting.rate_limiter as rl_module
import etlplus.api.retry_manager as rm_module
from etlplus.api import EndpointClient

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: FIXTURES ========================================================= #


@pytest.fixture
def client(
    base_url: str,
) -> EndpointClient:
    """
    Construct an :class:`EndpointClient` with retry enabled.

    Parameters
    ----------
    base_url : str
        Common base URL used across tests.

    Returns
    -------
    EndpointClient
        Client instance pointing at a dummy base URL and endpoint map.
    """
    return EndpointClient(
        base_url=base_url,
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
def rest_client_default(
    base_url: str,
) -> EndpointClient:
    """
    Default EndpointClient with no endpoints.

    Parameters
    ----------
    base_url : str
        Common base URL used across tests.

    Returns
    -------
    EndpointClient
        Configured EndpointClient instance.
    """
    return EndpointClient(
        base_url=base_url,
        endpoints={},
    )


@pytest.fixture
def rest_client_with_endpoints(
    base_url: str,
) -> EndpointClient:
    """
    EndpointClient with sample endpoints for API tests.

    Parameters
    ----------
    base_url : str
        Common base URL used across tests.

    Returns
    -------
    EndpointClient
        Configured EndpointClient instance.
    """
    return EndpointClient(
        base_url=base_url,
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
    # Patch the module-level sleep helper so :class:`RateLimiter` continues to
    # invoke ``time.sleep`` (allowing targeted tests to inspect it) without
    # pausing.
    monkeypatch.setattr(
        rl_module.time,
        'sleep',
        lambda _seconds: None,
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
    :class:`RateLimiter` to record rate-limit sleeps into the same list.
    """
    sleeps: list[float] = []

    # Patch RetryManager to inject a recording sleeper when none is given.
    original_init = rm_module.RetryManager.__init__

    def _init(self, *args, **kwargs):
        if 'sleeper' not in kwargs:

            def _sleeper(seconds: float) -> None:
                sleeps.append(seconds)

            kwargs['sleeper'] = _sleeper
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(
        rm_module.RetryManager,
        '__init__',
        _init,  # type: ignore[assignment]
    )

    # Patch :meth:`RateLimiter.enforce` so rate-limit sleeps are captured.
    def _capture_sleep(self: rl_module.RateLimiter) -> None:
        sleeps.append(self.sleep_seconds)

    monkeypatch.setattr(
        rl_module.RateLimiter,
        'enforce',
        _capture_sleep,
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
        rm_module.random,
        'uniform',
        fake_uniform,
    )
    return set_values
