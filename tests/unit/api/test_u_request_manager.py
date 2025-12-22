"""
:mod:`tests.unit.api.test_u_request_manager` module.

Unit tests covering :class:`etlplus.api.request_manager.RequestManager` adapter
plumbing.
"""

from __future__ import annotations

from typing import Any

import pytest

from etlplus.api.request_manager import RequestManager

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


class DummySession:
    """Lightweight session double tracking ``close`` calls."""

    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        """Close the session."""
        self.closed = True


# SECTION: TESTS ============================================================ #


def test_request_manager_builds_adapter_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure adapter configs yield a managed session that gets closed."""
    captured: dict[str, Any] = {}
    dummy_session = DummySession()

    def fake_builder(cfg: Any) -> DummySession:
        captured['cfg'] = cfg
        return dummy_session

    monkeypatch.setattr(
        'etlplus.api.request_manager.build_session_with_adapters',
        fake_builder,
    )

    manager = RequestManager(
        session_adapters=[{'prefix': 'https://', 'pool_connections': 2}],
    )

    def fake_request(
        _method: str,
        url: str,
        *,
        session: Any,
        timeout: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        captured['session'] = session
        captured['url'] = url
        captured['timeout'] = timeout
        captured['kwargs'] = kwargs
        return {'ok': True}

    result = manager.request(
        'GET',
        'https://example.com/resource',
        request_callable=fake_request,
    )

    assert result == {'ok': True}
    assert captured['session'] is dummy_session
    assert isinstance(captured['cfg'], tuple)
    assert dummy_session.closed is True


def test_request_manager_context_reuses_adapter_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Context manager should reuse one adapter-backed session."""
    dummy_session = DummySession()
    builder_calls: list[Any] = []
    sessions_used: list[Any] = []
    timeouts: list[Any] = []
    extra_kwargs: list[dict[str, Any]] = []

    def fake_builder(cfg: Any) -> DummySession:
        builder_calls.append(cfg)
        return dummy_session

    monkeypatch.setattr(
        'etlplus.api.request_manager.build_session_with_adapters',
        fake_builder,
    )

    manager = RequestManager(
        session_adapters=[{'prefix': 'https://', 'pool_connections': 1}],
    )

    def fake_request(
        _method: str,
        url: str,
        *,
        session: Any,
        timeout: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        sessions_used.append(session)
        timeouts.append(timeout)
        extra_kwargs.append(kwargs)
        return {'page': url}

    with manager:
        manager.request(
            'GET',
            'https://example.com/a',
            request_callable=fake_request,
        )
        manager.request(
            'GET',
            'https://example.com/b',
            request_callable=fake_request,
        )
        assert dummy_session.closed is False

    assert dummy_session.closed is True
    assert len(builder_calls) == 1
    assert sessions_used == [dummy_session, dummy_session]
    assert timeouts == [manager.default_timeout, manager.default_timeout]
    assert extra_kwargs == [{}, {}]
