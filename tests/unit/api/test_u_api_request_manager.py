"""
:mod:`tests.unit.api.test_u_api_request_manager` module.

Unit tests for :class:`etlplus.api._request_manager.RequestManager`.

These tests focus on:

- Session-adapter plumbing (building and closing sessions).
- Context-manager semantics (reuse + cleanup).
- Delegation to request callables.

The suite is intentionally lightweight and uses small doubles/mocks rather than
real network sessions.
"""

from __future__ import annotations

import types
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from typing import cast
from unittest.mock import Mock

import pytest
import requests  # type: ignore[import]

from etlplus.api._request_manager import RequestManager

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def _make_request_callable(
    probe: RequestProbe,
) -> Callable[..., dict[str, Any]]:
    """Create a request callable that records inputs into *probe*."""

    def _request(
        _method: str,
        url: str,
        *,
        session: Any,
        timeout: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        probe.sessions_used.append(session)
        probe.timeouts.append(timeout)
        probe.urls.append(url)
        probe.extra_kwargs.append(kwargs)
        return {'url': url}

    return _request


class _ResponseStub:
    """Minimal response stub for payload parsing and request_once tests."""

    def __init__(
        self,
        *,
        content_type: str = 'application/json',
        payload: Any = None,
        text: str = '',
        json_raises: bool = False,
    ) -> None:
        self.headers = {'content-type': content_type}
        self._payload = payload
        self.text = text
        self._json_raises = json_raises
        self.raise_called = False

    def json(self) -> Any:
        """Return payload or raise ValueError for malformed JSON."""
        if self._json_raises:
            raise ValueError('bad json')
        return self._payload

    def raise_for_status(self) -> None:
        """Track that status checks were performed."""
        self.raise_called = True


@dataclass(slots=True)
class DummySession:
    """Lightweight session double-tracking ``close`` calls."""

    closed: bool = False

    def close(self) -> None:
        """Close the session."""
        self.closed = True


@dataclass(slots=True)
class SessionBuilderProbe:
    """Callable probe for session-builder usage."""

    session: DummySession
    calls: list[Any]

    def __call__(self, cfg: Any) -> DummySession:
        self.calls.append(cfg)
        return self.session


@dataclass(slots=True)
class RequestProbe:
    """Callable probe for capturing arguments passed to a request callable."""

    sessions_used: list[Any]
    timeouts: list[Any]
    urls: list[str]
    extra_kwargs: list[dict[str, Any]]


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='dummy_session')
def dummy_session_fixture() -> DummySession:
    """Return a fresh dummy session for each test."""

    return DummySession()


@pytest.fixture(name='session_builder')
def session_builder_fixture(
    dummy_session: DummySession,
) -> SessionBuilderProbe:
    """Provide a probe callable for adapter-session creation."""

    return SessionBuilderProbe(session=dummy_session, calls=[])


# SECTION: TESTS ============================================================ #


class TestRequestManager:
    """Unit tests for :class:`etlplus.api._request_manager.RequestManager`."""

    def test_adapter_session_built_and_closed(
        self,
        monkeypatch: pytest.MonkeyPatch,
        session_builder: SessionBuilderProbe,
    ) -> None:
        """
        Test that adapter configs yield a managed session that gets closed.
        """

        monkeypatch.setattr(
            'etlplus.api._request_manager.build_session_with_adapters',
            session_builder,
        )

        manager = RequestManager(
            session_adapters=[{'prefix': 'https://', 'pool_connections': 2}],
        )

        probe = RequestProbe([], [], [], [])
        request_callable = _make_request_callable(probe)

        result = manager.request(
            'GET',
            'https://example.com/resource',
            request_callable=request_callable,
        )

        assert result == {'url': 'https://example.com/resource'}
        assert probe.sessions_used == [session_builder.session]
        assert isinstance(session_builder.calls[0], tuple)
        assert session_builder.session.closed is True

    def test_context_manager_reuses_adapter_session(
        self,
        monkeypatch: pytest.MonkeyPatch,
        session_builder: SessionBuilderProbe,
    ) -> None:
        """Test that context manager reuses one adapter-backed session."""

        monkeypatch.setattr(
            'etlplus.api._request_manager.build_session_with_adapters',
            session_builder,
        )

        manager = RequestManager(
            session_adapters=[{'prefix': 'https://', 'pool_connections': 1}],
        )

        probe = RequestProbe([], [], [], [])
        request_callable = _make_request_callable(probe)

        with manager:
            manager.request(
                'GET',
                'https://example.com/a',
                request_callable=request_callable,
            )
            manager.request(
                'GET',
                'https://example.com/b',
                request_callable=request_callable,
            )
            assert session_builder.session.closed is False

        assert session_builder.session.closed is True
        assert len(session_builder.calls) == 1
        assert probe.sessions_used == [session_builder.session] * 2
        assert probe.urls == ['https://example.com/a', 'https://example.com/b']
        assert probe.timeouts == [
            manager.default_timeout,
            manager.default_timeout,
        ]
        assert probe.extra_kwargs == [{}, {}]

    def test_invalid_session_adapters(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that bad adapter config does not raise during context enter/exit.
        """
        bad_adapters = cast(
            Any,
            [{'prefix': 'https://', 'pool_connections': 'bad'}],
        )
        manager = RequestManager(session_adapters=bad_adapters)

        def _bad_builder(cfg: Any) -> None:  # pragma: no cover
            raise ValueError('bad config')

        monkeypatch.setattr(
            'etlplus.api._request_manager.build_session_with_adapters',
            _bad_builder,
        )

        # If this raises, pytest will fail the test automatically.
        with manager:
            pass

    def test_request_delegates_to_request_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ``request`` passes through the ``request_callable`` to
        :meth:`request_once`.
        """

        manager = RequestManager()
        request_once = Mock(return_value={'ok': True})
        cb = Mock(return_value={'ok': 'cb'})

        monkeypatch.setattr(
            type(manager),
            'request_once',
            staticmethod(request_once),
        )

        result = manager.request('POST', 'http://test', request_callable=cb)

        assert result == {'ok': True}
        assert request_once.call_count == 1
        args = request_once.call_args.args
        kwargs = request_once.call_args.kwargs
        assert args[:2] == ('POST', 'http://test')
        assert kwargs['session'] is None
        assert kwargs['timeout'] == manager.default_timeout
        assert kwargs['request_callable'] is cb

    def test_default_init_values(self) -> None:
        """
        Test that :class:`RequestManager` default initialization values are
        stable and explicit.
        """
        manager = RequestManager()
        assert manager.retry is None
        assert manager.retry_network_errors is False
        assert manager.default_timeout == 10.0
        assert manager.session is None
        assert manager.session_factory is None
        assert manager.retry_cap == 30.0
        assert manager.session_adapters is None

    def test_context_manager_handles_exceptions(self) -> None:
        """
        Test that :meth:`__exit__` cleans up even when the managed block raises
        an exception.
        """
        manager = RequestManager()

        class DummyExc(Exception):
            """Dummy exception for context-manager testing."""

        manager._ctx_session = DummySession()
        manager._ctx_owns_session = True

        with pytest.raises(DummyExc), manager:
            raise DummyExc()

        assert manager._ctx_session is None
        assert manager._ctx_owns_session is False

    def test_request_once_returns_callable(self) -> None:
        """
        Test that :meth:`request_once` returns the underlying callable's
        result.
        """
        manager = RequestManager()

        def _callable(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return {'ok': True}

        result = manager.request_once(
            'GET',
            'http://x',
            session=None,
            timeout=1,
            request_callable=_callable,
        )

        assert result == {'ok': True}

    @pytest.mark.parametrize(
        ('api_method', 'expected_method'),
        [('get', 'GET'), ('post', 'POST')],
    )
    def test_http_shortcuts_delegate_to_request_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
        api_method: str,
        expected_method: str,
    ) -> None:
        """
        Test that ``GET``/``POST`` call into :meth:`request_once` with the
        right method.
        """

        manager = RequestManager()
        request_once = Mock(return_value={'ok': True})

        monkeypatch.setattr(
            type(manager),
            'request_once',
            staticmethod(request_once),
        )

        func = getattr(manager, api_method)
        assert callable(func)

        assert func('http://x') == {'ok': True}
        assert request_once.call_args.args[:2] == (expected_method, 'http://x')

    def test_request_accepts_unknown_methods(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that unknown HTTP method strings are passed through unchanged.
        """

        manager = RequestManager()
        request_once = Mock(return_value={'ok': True})

        monkeypatch.setattr(
            type(manager),
            'request_once',
            staticmethod(request_once),
        )

        assert manager.request('FOO', 'http://x') == {'ok': True}
        assert request_once.call_args.args[:2] == ('FOO', 'http://x')


class TestRequestManagerInternalPaths:
    """Extra branch coverage for internal request manager helpers."""

    def test_exit_ignores_missing_close_attribute(self) -> None:
        """
        Test that :meth:`__exit__` swallows AttributeError when close attribute
        is missing.
        """
        manager = RequestManager()
        manager._ctx_session = object()
        manager._ctx_owns_session = True

        manager.__exit__(None, None, None)

        assert manager._ctx_session is None
        assert manager._ctx_owns_session is False

    def test_exit_is_noop_when_context_session_missing(self) -> None:
        """
        Test that :meth:`__exit__` returns early when no context session is
        active.
        """
        manager = RequestManager()
        manager.__exit__(None, None, None)
        assert manager._ctx_session is None
        assert manager._ctx_owns_session is False

    def test_instantiate_session_handles_factory_none(self) -> None:
        """Test that a factory returning None yields no owned session."""
        manager = RequestManager(
            session_factory=cast(
                Callable[[], requests.Session],
                lambda: None,
            ),
        )
        resolved, owns = manager._instantiate_session()
        assert resolved is None
        assert owns is False

    @pytest.mark.parametrize(
        ('response', 'expected'),
        [
            pytest.param(
                _ResponseStub(
                    payload=None,
                    text='raw-json-text',
                    json_raises=True,
                ),
                {
                    'content': 'raw-json-text',
                    'content_type': 'application/json',
                },
                id='invalid-json-fallback',
            ),
            pytest.param(
                _ResponseStub(payload=[{'a': 1}, 2]),
                [{'a': 1}, {'value': 2}],
                id='list-payload-coercion',
            ),
            pytest.param(
                _ResponseStub(payload=3),
                {'value': 3},
                id='scalar-json-coercion',
            ),
            pytest.param(
                _ResponseStub(
                    content_type='text/plain',
                    payload='ignored',
                    text='plain-text',
                ),
                {'content': 'plain-text', 'content_type': 'text/plain'},
                id='non-json-content',
            ),
        ],
    )
    def test_parse_response_payload_branches(
        self,
        response: _ResponseStub,
        expected: Any,
    ) -> None:
        """
        Test that payload parser handles invalid, list, scalar, and non-JSON
        input.
        """
        manager = RequestManager()
        assert manager._parse_response_payload(cast(Any, response)) == expected

    def test_resolve_request_callable_raises_for_non_callable_session(
        self,
    ) -> None:
        """
        Test that non-callable :meth:`session.request` raises a clear
        :class:`TypeError`.
        """
        manager = RequestManager()
        bad_session = types.SimpleNamespace(request=123)
        with pytest.raises(TypeError, match='callable "request"'):
            manager._resolve_request_callable(
                cast(requests.Session, bad_session),
            )

    def test_request_once_without_custom_callable_parses_payload(self) -> None:
        """
        Test that :meth:`request_once` sends HTTP request, checks status, and
        parses JSON.
        """
        manager = RequestManager()
        response = _ResponseStub(payload={'ok': True})

        class _Session:
            @staticmethod
            def request(*_args: Any, **_kwargs: Any) -> _ResponseStub:
                """
                Simulate a session.request that returns our stub response.
                """
                return response

        result = manager.request_once(
            'get',
            'https://example.test/resource',
            session=cast(requests.Session, _Session()),
            timeout=2.5,
        )

        assert result == {'ok': True}
        assert response.raise_called is True

    def test_resolve_session_prefers_explicit_then_attached_session(
        self,
    ) -> None:
        """
        Test that session resolution honors explicit and configured sessions.
        """
        manager = RequestManager(
            session=cast(requests.Session, DummySession()),
        )
        explicit = cast(requests.Session, DummySession())
        resolved, owns = manager._resolve_session_for_call(explicit)
        assert resolved is explicit
        assert owns is False

        resolved2, owns2 = manager._resolve_session_for_call(None)
        assert resolved2 is manager.session
        assert owns2 is False

    def test_resolve_timeout_uses_explicit_value(self) -> None:
        """Test that explicit timeout bypasses default timeout fallback."""
        manager = RequestManager(default_timeout=10.0)
        assert manager._resolve_timeout(1.25) == 1.25

    def test_resolve_request_callable_defaults_to_requests_request(
        self,
    ) -> None:
        """
        Test that, if session is ``None``, the module-level
        :func:`requests.request` callable is used.
        """
        manager = RequestManager()
        assert manager._resolve_request_callable(None) is requests.request

    def test_send_http_request_normalizes_method_and_passes_timeout(
        self,
    ) -> None:
        """
        Test that low-level send helper uppercases method and forwards timeout.
        """
        manager = RequestManager()
        seen: dict[str, Any] = {}

        def _request(method: str, url: str, **kwargs: Any) -> _ResponseStub:
            seen['method'] = method
            seen['url'] = url
            seen['kwargs'] = kwargs
            return _ResponseStub(payload={'ok': True})

        session = cast(
            requests.Session,
            types.SimpleNamespace(request=_request),
        )
        manager._send_http_request(
            'post',
            'https://example.test/send',
            session=session,
            timeout=9,
            headers={'X-Test': '1'},
        )
        assert seen['method'] == 'POST'
        assert seen['url'] == 'https://example.test/send'
        assert seen['kwargs']['timeout'] == 9
        assert seen['kwargs']['headers'] == {'X-Test': '1'}
