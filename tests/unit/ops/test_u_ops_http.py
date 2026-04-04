"""
:mod:`tests.unit.ops.test_u_ops_http` module.

Unit tests for :mod:`etlplus.ops._http`.
"""

from __future__ import annotations

import importlib
from typing import Any

import pytest

from etlplus.api import HttpMethod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=protected-access

# SECTION: HELPERS ========================================================== #


http_mod = importlib.import_module('etlplus.ops._http')


class _Response:
    """Minimal response stub for payload parsing tests."""

    def __init__(
        self,
        payload: object,
        *,
        text: str = 'fallback',
        json_error: bool = False,
    ) -> None:
        self._payload = payload
        self.text = text
        self._json_error = json_error

    def json(self) -> object:
        """Return the preset payload or raise a decode error."""
        if self._json_error:
            raise ValueError('bad json')
        return self._payload

    def raise_for_status(self) -> None:
        """No-op status check for request execution tests."""
        return None


# SECTION: TESTS ============================================================ #


class TestBuildDirectRequestEnv:
    """Unit tests for direct-request environment construction."""

    def test_build_direct_request_env_splits_session_and_timeout(self) -> None:
        """Session/timeout should be promoted out of request kwargs."""
        env = http_mod.build_direct_request_env(
            'https://example.test/api',
            HttpMethod.PUT,
            {
                'session': 'session',
                'timeout': 2.5,
                'headers': {'X-Test': '1'},
                'verify': False,
            },
        )

        assert env == {
            'url': 'https://example.test/api',
            'method': HttpMethod.PUT,
            'timeout': 2.5,
            'session': 'session',
            'request_kwargs': {
                'headers': {'X-Test': '1'},
                'verify': False,
            },
        }

    def test_build_direct_request_env_defaults_optional_fields(self) -> None:
        """Missing session/timeout should normalize to ``None``."""
        env = http_mod.build_direct_request_env(
            'https://example.test/api',
            HttpMethod.GET,
        )

        assert env == {
            'url': 'https://example.test/api',
            'method': HttpMethod.GET,
            'timeout': None,
            'session': None,
            'request_kwargs': {},
        }


class TestBuildRequestCall:
    """Unit tests for normalized request-call construction."""

    def test_build_request_call_keeps_explicit_request_headers(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that explicit request kwargs headers win over env headers."""
        monkeypatch.setattr(
            http_mod,
            'resolve_request',
            lambda method, session, timeout: (
                lambda url, **kwargs: None,
                timeout,
                HttpMethod.POST,
            ),
        )

        result = http_mod.build_request_call(
            {
                'url': 'https://example.test/api',
                'headers': {'X-Env': 'env'},
                'request_kwargs': {'headers': {'X-Request': 'request'}},
            },
            error_message='missing',
            default_method=HttpMethod.POST,
        )

        assert result.kwargs['headers'] == {'X-Request': 'request'}

    def test_build_request_call_merges_headers_and_json_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that headers, JSON payload, and resolved method details survive.
        """
        captured: dict[str, Any] = {}

        def _callable(url: str, **kwargs: Any) -> None:
            captured['url'] = url
            captured['kwargs'] = kwargs

        monkeypatch.setattr(
            http_mod,
            'resolve_request',
            lambda method, session, timeout: (
                _callable,
                9.5,
                HttpMethod.PUT,
            ),
        )

        result = http_mod.build_request_call(
            {
                'url': 'https://example.test/api',
                'headers': {'X-Test': '1'},
                'method': 'put',
                'request_kwargs': {'verify': False},
                'session': 'session',
                'timeout': 2.0,
            },
            error_message='missing',
            default_method=HttpMethod.POST,
            json_data={'ok': True},
        )

        assert result.url == 'https://example.test/api'
        assert result.timeout == 9.5
        assert result.http_method is HttpMethod.PUT
        assert result.kwargs == {
            'headers': {'X-Test': '1'},
            'json': {'ok': True},
            'verify': False,
        }


class TestRequireUrl:
    """Unit tests for required URL extraction."""

    @pytest.mark.parametrize(
        'env',
        [{}, {'url': ''}, {'url': None}, {'url': 7}],
    )
    def test_require_url_rejects_missing_or_invalid_values(
        self,
        env: dict[str, object],
    ) -> None:
        """Test that missing or non-string URLs raise :class:`ValueError`."""
        with pytest.raises(ValueError, match='missing'):
            http_mod.require_url(env, error_message='missing')

    @pytest.mark.parametrize(
        ('env', 'expected'),
        [
            ({'url': 'https://example.test'}, 'https://example.test'),
        ],
    )
    def test_require_url_returns_string(
        self,
        env: dict[str, object],
        expected: str,
    ) -> None:
        """Test that string URL values are returned unchanged."""
        assert http_mod.require_url(env, error_message='missing') == expected


class TestResponseJsonOrText:
    """Unit tests for response payload parsing."""

    def test_response_json_or_text_falls_back_to_text(self) -> None:
        """Test that JSON decode failures return raw response text."""
        assert (
            http_mod.response_json_or_text(
                _Response({'ok': True}, text='fallback', json_error=True),
            )
            == 'fallback'
        )

    def test_response_json_or_text_prefers_json_payload(self) -> None:
        """JSON-capable responses should return their parsed payload."""
        assert http_mod.response_json_or_text(_Response({'ok': True})) == {
            'ok': True,
        }


class TestSendRequest:
    """Unit tests for normalized request execution."""

    def test_send_request_executes_normalized_call(self) -> None:
        """Test that request execution preserves URL, timeout, and kwargs."""
        captured: dict[str, Any] = {}

        def _request_callable(url: str, **kwargs: Any) -> _Response:
            captured['url'] = url
            captured['kwargs'] = kwargs
            return _Response({'ok': True})

        response = http_mod.send_request(
            http_mod.ResolvedRequest(
                url='https://example.test/api',
                request_callable=_request_callable,
                timeout=4.5,
                http_method=HttpMethod.POST,
                kwargs={'headers': {'X-Test': '1'}},
            ),
        )

        assert response.json() == {'ok': True}
        assert captured == {
            'url': 'https://example.test/api',
            'kwargs': {
                'headers': {'X-Test': '1'},
                'timeout': 4.5,
            },
        }
