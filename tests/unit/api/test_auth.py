"""
tests.unit.api.auth unit tests module.

Smoke tests for etlplus.api.auth EndpointCredentialsBearer.
"""
from __future__ import annotations

import time
import types
from typing import Any

import pytest
import requests  # type: ignore[import]

from etlplus.api.auth import CLOCK_SKEW_SEC
from etlplus.api.auth import EndpointCredentialsBearer


# SECTION: HELPERS ========================================================== #


class _Resp:
    def __init__(
        self,
        payload: dict[str, Any],
        status: int = 200,
    ) -> None:
        self._payload = payload
        self.status_code = status
        self.text = str(payload)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            err = requests.HTTPError('boom')
            err.response = types.SimpleNamespace(
                status_code=self.status_code,
                text=self.text,
            )
            raise err

    def json(self) -> dict[str, Any]:
        return self._payload


# SECTION: FIXTURES ========================================================= #


@pytest.fixture
def token_sequence(monkeypatch: pytest.MonkeyPatch):
    calls = {'n': 0}

    def fake_post(
        url: str,
        data: dict[str, Any],
        auth,
        headers,
        timeout,
    ):  # noqa: D401, ANN001
        calls['n'] += 1
        return _Resp({'access_token': f"t{calls['n']}", 'expires_in': 60})

    monkeypatch.setattr(requests, 'post', fake_post)
    return calls


# SECTION: TESTS ============================================================ #


def test_bearer_fetches_and_caches(token_sequence):  # noqa: ANN001
    auth = EndpointCredentialsBearer(
        token_url='https://auth.example.com/token',
        client_id='id',
        client_secret='secret',
        scope='read',
    )
    s = requests.Session()
    s.auth = auth

    r1 = requests.Request('GET', 'https://api.example.com/x').prepare()
    s.auth(r1)
    assert r1.headers.get('Authorization') == 'Bearer t1'

    # Second call should not fetch a new token (still valid)
    r2 = requests.Request('GET', 'https://api.example.com/y').prepare()
    s.auth(r2)
    assert r2.headers.get('Authorization') == 'Bearer t1'
    assert token_sequence['n'] == 1


def test_bearer_refreshes_when_expiring(
    monkeypatch: pytest.MonkeyPatch,
):  # noqa: D401
    calls = {'n': 0}

    def fake_post(
        url: str,
        data: dict[str, Any],
        auth,
        headers,
        timeout,
    ):  # noqa: D401, ANN001
        calls['n'] += 1
        # First token almost expired; second token longer lifetime
        if calls['n'] == 1:
            return _Resp(
                {'access_token': 'short', 'expires_in': CLOCK_SKEW_SEC - 1},
            )
        return _Resp({'access_token': 'long', 'expires_in': 120})

    monkeypatch.setattr(requests, 'post', fake_post)

    auth = EndpointCredentialsBearer(
        token_url='https://auth.example.com/token',
        client_id='id',
        client_secret='secret',
        scope='read',
    )

    # First call obtains short token
    r1 = requests.Request('GET', 'https://api.example.com/x').prepare()
    auth(r1)
    assert r1.headers['Authorization'] == 'Bearer short'

    # Force time forward to trigger refresh logic
    monkeypatch.setattr(
        time,
        'time',
        lambda: auth.expiry - (CLOCK_SKEW_SEC / 2),
    )

    r2 = requests.Request('GET', 'https://api.example.com/y').prepare()
    auth(r2)
    assert r2.headers['Authorization'] == 'Bearer long'
    assert calls['n'] == 2
