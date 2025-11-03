"""
ETLPlus Retry Network Errors
=======================

Unit tests for the ETLPlus retry network error handling.
"""
from __future__ import annotations

from typing import Any

import requests

from etlplus.api import EndpointClient


def test_retry_on_network_errors(monkeypatch) -> None:
    """
    When retry_network_errors=True, the client should retry on timeouts and
    connection errors using jittered backoff, then succeed.
    """

    sleeps: list[float] = []

    # Capture sleep calls
    monkeypatch.setattr(
        EndpointClient,
        'apply_sleep',
        staticmethod(lambda s, *, sleeper=None: sleeps.append(s)),
        raising=False,
    )

    # Deterministic jitter
    import etlplus.api.client as client_mod

    jitter_vals = iter([0.12, 0.18])
    monkeypatch.setattr(
        client_mod.random,
        'uniform',
        lambda a, b: next(jitter_vals),
    )

    # Simulate Timeout then ConnectionError then success
    attempts = {'n': 0}

    def _fake_extract(_stype: str, _url: str, **_kw: Any) -> Any:
        attempts['n'] += 1
        if attempts['n'] == 1:
            raise requests.Timeout('slow')
        if attempts['n'] == 2:
            raise requests.ConnectionError('reset')
        return {'ok': True}

    monkeypatch.setattr(client_mod, '_extract', _fake_extract)

    c = EndpointClient(
        base_url='https://api.example.com',
        endpoints={},
        retry={'max_attempts': 4, 'backoff': 0.5},
        retry_network_errors=True,
    )

    out = c._extract_with_retry('https://api.example.com/items')
    assert out == {'ok': True}

    # Should have slept twice (after 2 failures)
    assert sleeps == [0.12, 0.18]
    assert attempts['n'] == 3
