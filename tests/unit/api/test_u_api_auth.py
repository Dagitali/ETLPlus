"""
:mod:`tests.unit.api.test_u_auth` module.

Unit tests for :mod:`etlplus.api._auth`.

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

from etlplus.api._auth import CLOCK_SKEW_SEC
from etlplus.api._auth import EndpointCredentialsBearer

from ...pytest_shared_support import RequestFactory

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: FIXTURES ========================================================= #


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

    @pytest.mark.parametrize(
        ('text', 'json_payload', 'expected_token', 'expected_error'),
        [
            pytest.param(
                '[]',
                ['not', 'a', 'mapping'],
                None,
                ValueError,
                id='non-mapping-payload',
            ),
            pytest.param(
                '{"access_token":"abc","expires_in":"bad"}',
                {'access_token': 'abc', 'expires_in': 'bad'},
                'abc',
                None,
                id='invalid-ttl',
            ),
        ],
    )
    def test_parse_token_response(
        self,
        bearer_factory: Callable[..., EndpointCredentialsBearer],
        text: str,
        json_payload: object,
        expected_token: str | None,
        expected_error: type[Exception] | None,
    ) -> None:
        """Test token response parsing validation and TTL normalization."""
        auth = bearer_factory()
        resp = types.SimpleNamespace(
            text=text,
            json=lambda: json_payload,
        )

        if expected_error is not None:
            with pytest.raises(expected_error, match='JSON object'):
                auth._parse_token_response(cast(Any, resp))
            return

        parsed = auth._parse_token_response(cast(Any, resp))
        assert parsed['access_token'] == expected_token
        assert parsed['expires_in'] > 0

    def test_post_error_paths_raise(
        self,
        monkeypatch: pytest.MonkeyPatch,
        bearer_factory: Callable[..., EndpointCredentialsBearer],
        request_factory: RequestFactory,
        auth_post_error_case: tuple[Callable[..., Any], type[Exception]],
    ) -> None:
        """Test that various POST failure paths raise the expected errors."""

        post_callable, expected_exception = auth_post_error_case
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
        fake_response_factory: Callable[..., Any],
    ) -> None:
        """
        Test that :class:`EndpointCredentialsBearer` refreshes token when
        expiring.
        """
        calls: dict[str, int] = {'n': 0}

        def fake_post(
            *args,
            **kwargs,
        ) -> Any:
            calls['n'] += 1

            # First token almost expired; second token longer lifetime.
            if calls['n'] == 1:
                return fake_response_factory(
                    payload={
                        'access_token': 'short',
                        'expires_in': CLOCK_SKEW_SEC - 1,
                    },
                )
            return fake_response_factory(
                payload={'access_token': 'long', 'expires_in': 120},
            )

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

    def test_request_token_exception_branches(
        self,
        bearer_factory: Callable[..., EndpointCredentialsBearer],
        auth_request_exception_case: tuple[
            requests.RequestException,
            type[Exception],
        ],
    ) -> None:
        """
        Test that token request exceptions propagate with branch coverage.
        """
        exc, expected = auth_request_exception_case

        class _Client:
            @staticmethod
            def post(*_args: Any, **_kwargs: Any) -> Any:
                raise exc

        auth = bearer_factory()
        auth.session = cast(requests.Session, _Client())
        with pytest.raises(expected):
            auth._request_token()

    @pytest.mark.parametrize(
        ('scope', 'expected'),
        [
            pytest.param('   ', {'grant_type': 'client_credentials'}, id='blank'),
            pytest.param(None, {'grant_type': 'client_credentials'}, id='missing'),
            pytest.param(
                'read',
                {'grant_type': 'client_credentials', 'scope': 'read'},
                id='configured',
            ),
        ],
    )
    def test_token_payload_scope_variants(
        self,
        bearer_factory: Callable[..., EndpointCredentialsBearer],
        scope: str | None,
        expected: dict[str, str],
    ) -> None:
        """Test scope inclusion rules for token payloads."""
        auth = bearer_factory(scope=scope)
        assert auth._token_payload() == expected
