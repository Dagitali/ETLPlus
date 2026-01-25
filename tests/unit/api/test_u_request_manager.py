"""
:mod:`tests.unit.api.test_u_request_manager` module.

Unit tests for :class:`etlplus.api.request_manager.RequestManager`.

These tests focus on:

- Session-adapter plumbing (building and closing sessions).
- Context-manager semantics (reuse + cleanup).
- Delegation to request callables.

The suite is intentionally lightweight and uses small doubles/mocks rather than
real network sessions.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from typing import cast
from unittest.mock import Mock

import pytest

from etlplus.api.request_manager import RequestManager

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


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
    """Unit tests for :class:`etlplus.api.request_manager.RequestManager`."""

    def test_adapter_session_built_and_closed(
        self,
        monkeypatch: pytest.MonkeyPatch,
        session_builder: SessionBuilderProbe,
    ) -> None:
        """
        Test that adapter configs yield a managed session that gets closed.
        """

        monkeypatch.setattr(
            'etlplus.api.request_manager.build_session_with_adapters',
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
            'etlplus.api.request_manager.build_session_with_adapters',
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
            'etlplus.api.request_manager.build_session_with_adapters',
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
        Test that :meth:`__exit__` cleans up even when the managed block
        raises an exception.
        """
        # pylint: disable=protected-access

        manager = RequestManager()

        class DummyExc(Exception):
            """Dummy exception for context-manager testing."""

        manager._ctx_session = DummySession()
        manager._ctx_owns_session = True

        with pytest.raises(DummyExc):
            with manager:
                raise DummyExc()

        assert manager._ctx_session is None
        assert manager._ctx_owns_session is False

    def test_request_once_returns_callable(self) -> None:
        """
        Test that :meth:`request_once` returns the underlying callable's
        result.
        """
        # pylint: disable=unused-argument

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
