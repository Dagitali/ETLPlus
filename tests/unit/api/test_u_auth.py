"""
:mod:`tests.unit.api.test_u_auth` module.

Unit tests for ``etlplus.api.auth``.

Notes
-----
- Uses a lightweight fake response object and monkey-patched session.
- Simulates expiration and refresh timing windows via ``time.time`` patch.
- Validates raising behavior for non-200 authentication responses.

Examples
--------
>>> pytest tests/unit/api/test_u_auth.py
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
    """
    Lightweight fake response object for simulating requests.Response.

    Parameters
    ----------
    payload : dict[str, Any]
        JSON payload to return.
    status : int, optional
        HTTP status code (default: 200).
    """

    def __init__(
        self,
        payload: dict[str, Any],
        status: int = 200,
    ) -> None:
        self._payload = payload
        self.status_code = status
        self.text = str(payload)

    def raise_for_status(self) -> None:
        """Raise HTTPError if status code indicates an error."""
        if self.status_code >= 400:
            err = requests.HTTPError('boom')
            err.response = types.SimpleNamespace(
                status_code=self.status_code,
                text=self.text,
            )
            raise err

    def json(self) -> dict[str, Any]:
        """Return the JSON payload."""
        return self._payload


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='token_sequence')
def token_sequence_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, int]:
    """
    Track token fetch count and patch requests.post for token acquisition.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.

    Returns
    -------
    dict[str, int]
        Dictionary tracking token fetch count.
    """
    # pylint: disable=unused-argument

    calls: dict[str, int] = {'n': 0}

    def fake_post(
        *args,
        **kwargs,
    ) -> _Resp:
        calls['n'] += 1
        return _Resp({'access_token': f't{calls["n"]}', 'expires_in': 60})

    monkeypatch.setattr(requests, 'post', fake_post)

    return calls


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestEndpointCredentialsBearer:
    """
    Unit test suite for :class:`EndpointCredentialsBearer`.

    Notes
    -----
    - Tests authentication logic.
    - Uses monkeypatching to simulate token fetch, expiration, and error paths.
    - Validates caching, refresh, and error handling behavior.
    """

    def test_fetches_and_caches(
        self,
        token_sequence: dict[str, int],
    ) -> None:
        """
        Test that :class:`EndpointCredentialsBearer` fetches and caches tokens
        correctly.

        Parameters
        ----------
        token_sequence : dict[str, int]
            Fixture tracking token fetch count.
        """
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

        # Second call should not fetch a new token (still valid).
        r2 = requests.Request('GET', 'https://api.example.com/y').prepare()
        s.auth(r2)
        assert r2.headers.get('Authorization') == 'Bearer t1'
        assert token_sequence['n'] == 1

    def test_refreshes_when_expiring(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :class:`EndpointCredentialsBearer` refreshes token when
        expiring.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        """
        # pylint: disable=unused-argument

        calls: dict[str, int] = {'n': 0}

        def fake_post(
            *args,
            **kwargs,
        ) -> _Resp:
            calls['n'] += 1

            # First token almost expired; second token longer lifetime.
            if calls['n'] == 1:
                return _Resp(
                    {
                        'access_token': 'short',
                        'expires_in': CLOCK_SKEW_SEC - 1,
                    },
                )
            return _Resp({'access_token': 'long', 'expires_in': 120})

        monkeypatch.setattr(requests, 'post', fake_post)

        auth = EndpointCredentialsBearer(
            token_url='https://auth.example.com/token',
            client_id='id',
            client_secret='secret',
            scope='read',
        )

        # First call obtains short token.
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

    def test_http_error_path_raises_http_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that HTTP error path raises HTTPError.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        """
        # pylint: disable=protected-access
        # pylint: disable=unused-argument

        def fake_post(
            *args,
            **kwargs,
        ):
            # Simulate HTTP 401 with body; requests raises on raise_for_status.
            resp = requests.Response()
            resp.status_code = 401
            resp._content = b'Unauthorized'  # type: ignore[attr-defined]

            class _R:
                def raise_for_status(self):
                    """Raise HTTPError for 401 response."""
                    e = requests.HTTPError('401')
                    e.response = resp  # type: ignore[attr-defined]
                    raise e

            return _R()

        monkeypatch.setattr(requests, 'post', fake_post)
        auth = EndpointCredentialsBearer(
            token_url='https://auth.example.com/token',
            client_id='id',
            client_secret='secret',
            scope='read',
        )
        r = requests.Request('GET', 'https://api.example.com/x').prepare()
        with pytest.raises(requests.HTTPError):
            auth(r)

    def test_missing_access_token_raises_runtime_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that missing access_token raises RuntimeError.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        """
        # pylint: disable=unused-argument

        def fake_post(
            *args,
            **kwargs,
        ) -> _Resp:
            return _Resp({'expires_in': 60})  # No access_token.

        monkeypatch.setattr(requests, 'post', fake_post)
        auth = EndpointCredentialsBearer(
            token_url='https://auth.example.com/token',
            client_id='id',
            client_secret='secret',
            scope='read',
        )
        r = requests.Request('GET', 'https://api.example.com/x').prepare()
        with pytest.raises(RuntimeError):
            auth(r)

    def test_non_json_body_raises_value_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that non-JSON body raises ValueError.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        """

        class _R:
            status_code = 200
            text = 'not json'

            def raise_for_status(self):
                """Raise nothing for HTTP 200 OK status."""
                return None

            def json(self):
                """Raise ValueError for invalid JSON."""
                raise ValueError('invalid json')

        monkeypatch.setattr(requests, 'post', lambda *a, **k: _R())

        auth = EndpointCredentialsBearer(
            token_url='https://auth.example.com/token',
            client_id='id',
            client_secret='secret',
            scope='read',
        )
        r = requests.Request('GET', 'https://api.example.com/x').prepare()
        with pytest.raises(ValueError):
            auth(r)
