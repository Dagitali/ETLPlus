"""
ETLPlus Retry Backoffs
=======================

Unit tests for the ETLPlus retry backoff mechanism.
"""
from __future__ import annotations

import types
from typing import Any

import requests

from etlplus.api.client import EndpointClient


class _Resp:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


def test_retry_backoff_full_jitter(monkeypatch) -> None:
    """
    Ensure the retry wrapper uses jittered backoff and sleeps the sampled
    values between attempts. We simulate two failures (503) then success.
    """

    sleeps: list[float] = []

    # Patch apply_sleep to capture sleep durations (avoid real sleep)
    monkeypatch.setattr(
        EndpointClient,
        'apply_sleep',
        staticmethod(lambda s, *, sleeper=None: sleeps.append(s)),
        raising=False,
    )

    # Patch random.uniform to deterministic sequence
    import etlplus.api.client as client_mod

    uniform_vals = iter([0.1, 0.2])
    monkeypatch.setattr(
        client_mod.random,
        'uniform',
        lambda a, b: next(uniform_vals),
    )

    # Patch _extract in client module to fail with 503 twice, then succeed
    attempts = {'n': 0}

    def _fake_extract(_stype: str, _url: str, **_kw: Any) -> Any:
        attempts['n'] += 1
        if attempts['n'] < 3:
            err = requests.HTTPError('boom')
            # attach response with status_code=503
            err.response = types.SimpleNamespace(  # type: ignore[attr-defined]
                status_code=503,
            )
            raise err
        return {'ok': True}

    monkeypatch.setattr(client_mod, '_extract', _fake_extract)

    c = EndpointClient(
        base_url='https://api.example.com',
        endpoints={},
        retry={'max_attempts': 4, 'backoff': 0.5, 'retry_on': [503]},
    )

    out = c._extract_with_retry('https://api.example.com/items')
    assert out == {'ok': True}

    # We should have slept twice (between the three attempts)
    assert sleeps == [0.1, 0.2]
    # Sanity: ensure attempts captured
    assert attempts['n'] == 3
