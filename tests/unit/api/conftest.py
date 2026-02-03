"""
:mod:`tests.unit.api.conftest` module.

Define shared fixtures and helpers for pytest-based unit tests of
:mod:`etlplus.api`.

Notes
-----
- Fixtures are designed for reuse and DRY test setup across API-focused unit
    tests.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

import etlplus.api.rate_limiting.rate_limiter as rl_module
import etlplus.api.retry_manager as rm_module

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: FIXTURES ========================================================= #


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


    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.

    Returns
    -------
    Callable[[list[float]], list[float]]
        Function that sets the sequence of jitter values for
        ``RetryManager.random.uniform``.

    Examples
    --------
    >>> vals = jitter([0.1, 0.2])
    ... # Now retry jitter will use 0.1, then 0.2 for uniform(a, b)
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
