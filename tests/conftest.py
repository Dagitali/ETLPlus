"""
tests.conftest.py test fixtures module.

Shared pytest fixtures for the test suite.

Provides reusable factory for `EndpointClient` and a sleep capture fixture
used by retry logic tests.
"""
from __future__ import annotations

from typing import Any
from typing import Callable

import pytest

from etlplus.api.client import EndpointClient


# SECTION: FIXTURES ========================================================= #


@pytest.fixture
def capture_sleeps(
    monkeypatch: pytest.MonkeyPatch,
) -> list[float]:
    """
    Capture applied sleep durations from retry backoff logic.

    Patches `EndpointClient.apply_sleep` so tests can assert jitter/backoff
    behavior without actually waiting.
    """
    values: list[float] = []

    def _sleep(s: float, *, _sleeper=None) -> None:  # noqa: D401, ANN001
        values.append(s)

    monkeypatch.setattr(
        EndpointClient,
        'apply_sleep',
        staticmethod(_sleep),
        raising=False,
    )

    return values


@pytest.fixture
def client_factory() -> Callable[..., EndpointClient]:
    """
    Return a factory to build `EndpointClient` instances.

    Parameters can be overridden per test. `endpoints` defaults to an empty
    mapping to simplify most calls.
    """
    def _make(
        *,
        base_url: str = 'https://api.example.com',
        endpoints: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> EndpointClient:
        return EndpointClient(
            base_url=base_url,
            endpoints=endpoints or {},
            **kwargs,
        )

    return _make
