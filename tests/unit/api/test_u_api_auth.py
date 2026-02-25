"""
:mod:`tests.unit.api.test_u_auth` module.

Unit tests for :mod:`etlplus.api.auth`.

Notes
-----
- Uses a lightweight fake response object and monkey-patched session.
- Simulates expiration and refresh timing windows via ``time.time`` patch.
- Validates raising behavior for non-200 authentication responses.

Examples
--------
>>> pytest tests/unit/api/test_u_api_auth.py
"""

from __future__ import annotations

import time
import types
from collections.abc import Callable
from typing import Any
from typing import cast

import pytest
import requests  # type: ignore[import]

from etlplus.api.auth import CLOCK_SKEW_SEC
from etlplus.api.auth import EndpointCredentialsBearer
from etlplus.api.auth import _truncate

from ...conftest import RequestFactory

# SECTION: TYPES ============================================================ #


class _NonJsonResponse:
    """Response stub that raises :class:`ValueError` from ``json``."""

    status_code = 200
    text = 'not json'

    @staticmethod
    def raise_for_status() -> None:
        """HTTP 200 OK; no-op."""

    @staticmethod
    def json() -> dict[str, Any]:
        """Raise ``ValueError`` to simulate malformed JSON payloads."""
        raise ValueError('invalid json')


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


@pytest.fixture(name='bearer_factory')
def bearer_factory_fixture() -> Callable[..., EndpointCredentialsBearer]:
    """
    Factory fixture that builds :class:`EndpointCredentialsBearer` objects.
    """

    def _make(**overrides: Any) -> EndpointCredentialsBearer:
        params: dict[str, Any] = {
            'token_url': 'https://auth.example.com/token',
            'client_id': 'id',
            'client_secret': 'secret',
            'scope': 'read',
        }
        params.update(overrides)
        return EndpointCredentialsBearer(**params)

    return _make


@pytest.fixture(name='request_factory')
def request_factory_fixture(
    base_url: str,
) -> RequestFactory:
    """Factory that builds prepared GET requests for the auth tests."""

    def _make(
        url: str | None = None,
    ) -> requests.PreparedRequest:
        target = url or f'{base_url}/x'
        return requests.Request('GET', target).prepare()

    return _make


# SECTION: TESTS ============================================================ #


class TestEndpointCredentialsBearer:
    """
    Unit tests for :class:`EndpointCredentialsBearer`.

    Notes
    -----
    - Tests authentication logic.
    - Uses monkeypatching to simulate token fetch, expiration, and error paths.
    - Validates caching, refresh, and error handling behavior.
    """

    # pylint: disable=protected-access

    def test_fetches_and_caches(
        self,
        base_url: str,
        token_sequence: dict[str, int],
        bearer_factory: Callable[..., EndpointCredentialsBearer],
        request_factory: RequestFactory,
    ) -> None:
        """
        Test that :class:`EndpointCredentialsBearer` fetches and caches tokens
        correctly.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        token_sequence : dict[str, int]
            Fixture tracking token fetch count.
        bearer_factory : Callable[..., EndpointCredentialsBearer]
            Factory that builds :class:`EndpointCredentialsBearer` objects.
        request_factory : RequestFactory
            Factory that builds prepared requests.
        """
        auth = bearer_factory()
        s = requests.Session()
        s.auth = auth

        r1 = request_factory()
        s.auth(r1)
        assert r1.headers.get('Authorization') == 'Bearer t1'

        # Second call should not fetch a new token (still valid).
        r2 = request_factory(f'{base_url}/y')
        s.auth(r2)
        assert r2.headers.get('Authorization') == 'Bearer t1'
        assert token_sequence['n'] == 1

    def test_parse_token_response_requires_mapping_payload(
        self,
        bearer_factory: Callable[..., EndpointCredentialsBearer],
    ) -> None:
        """Non-mapping token payloads should raise ValueError."""
        auth = bearer_factory()
        resp = types.SimpleNamespace(
            text='[]',
            json=lambda: ['not', 'a', 'mapping'],
        )
        with pytest.raises(ValueError, match='JSON object'):
            auth._parse_token_response(cast(Any, resp))

    def test_parse_token_response_falls_back_default_ttl(
        self,
        bearer_factory: Callable[..., EndpointCredentialsBearer],
    ) -> None:
        """Invalid expires_in values should coerce to default TTL."""
        auth = bearer_factory()
        resp = types.SimpleNamespace(
            text='{"access_token":"abc","expires_in":"bad"}',
            json=lambda: {'access_token': 'abc', 'expires_in': 'bad'},
        )
        parsed = auth._parse_token_response(cast(Any, resp))
        assert parsed['access_token'] == 'abc'
        assert parsed['expires_in'] > 0

    @pytest.mark.parametrize(
        ('post_callable', 'expected_exception'),
        [
            pytest.param(
                lambda *_a, **_k: _Resp({}, status=401),
                requests.HTTPError,
                id='http-error',
            ),
            pytest.param(
                lambda *_a, **_k: _Resp({'expires_in': 60}),
                RuntimeError,
                id='missing-token',
            ),
            pytest.param(
                lambda *_a, **_k: _NonJsonResponse(),
                ValueError,
                id='invalid-json',
            ),
        ],
    )
    def test_post_error_paths_raise(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bearer_factory: Callable[..., EndpointCredentialsBearer],
        request_factory: RequestFactory,
        post_callable: Callable[..., Any],
        expected_exception: type[Exception],
    ) -> None:
        """Test that various POST failure paths raise the expected errors."""

        monkeypatch.setattr(requests, 'post', post_callable)
        auth = bearer_factory()
        request = request_factory()
        with pytest.raises(expected_exception):
            auth(request)

    def test_refreshes_when_expiring(
        self,
        base_url: str,
        monkeypatch: pytest.MonkeyPatch,
        bearer_factory: Callable[..., EndpointCredentialsBearer],
        request_factory: RequestFactory,
    ) -> None:
        """
        Test that :class:`EndpointCredentialsBearer` refreshes token when
        expiring.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        bearer_factory : Callable[..., EndpointCredentialsBearer]
            Factory that builds :class:`EndpointCredentialsBearer` objects.
        request_factory : RequestFactory
            Factory that builds prepared requests.
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

        auth = bearer_factory()

        # First call obtains short token.
        r1 = request_factory()
        auth(r1)
        assert r1.headers['Authorization'] == 'Bearer short'

        # Force time forward to trigger refresh logic
        monkeypatch.setattr(
            time,
            'time',
            lambda: auth.expiry - (CLOCK_SKEW_SEC / 2),
        )

        r2 = request_factory(f'{base_url}/y')
        auth(r2)
        assert r2.headers['Authorization'] == 'Bearer long'
        assert calls['n'] == 2

    @pytest.mark.parametrize(
        ('exc', 'expected'),
        [
            pytest.param(
                requests.exceptions.Timeout('timeout'),
                requests.exceptions.Timeout,
                id='timeout',
            ),
            pytest.param(
                requests.exceptions.SSLError('ssl'),
                requests.exceptions.SSLError,
                id='ssl',
            ),
            pytest.param(
                requests.exceptions.ConnectionError('conn'),
                requests.exceptions.ConnectionError,
                id='connection',
            ),
            pytest.param(
                requests.exceptions.RequestException('generic'),
                requests.exceptions.RequestException,
                id='request-exception',
            ),
        ],
    )
    def test_request_token_exception_branches(
        self,
        bearer_factory: Callable[..., EndpointCredentialsBearer],
        exc: Exception,
        expected: type[Exception],
    ) -> None:
        """Token request exceptions should propagate with branch coverage."""

        class _Client:
            @staticmethod
            def post(*_args: Any, **_kwargs: Any) -> Any:
                raise exc

        auth = bearer_factory()
        auth.session = cast(requests.Session, _Client())
        with pytest.raises(expected):
            auth._request_token()

    def test_truncate_empty_text_returns_empty_string(self) -> None:
        """Empty/None input should truncate to an empty string."""
        assert _truncate('') == ''
        assert _truncate(None) == ''

    def test_token_payload_omits_blank_scope(
        self,
        bearer_factory: Callable[..., EndpointCredentialsBearer],
    ) -> None:
        """Whitespace-only scope should not be included in token payload."""
        auth = bearer_factory(scope='   ')
        assert auth._token_payload() == {'grant_type': 'client_credentials'}
