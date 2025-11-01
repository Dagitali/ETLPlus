"""
ETLPlus Client Session Tests
============================

Unit tests for the ETLPlus client session.
"""
from __future__ import annotations

from typing import Any

from etlplus.api.client import EndpointClient


class FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload
        self.headers = {'content-type': 'application/json'}

    def raise_for_status(self) -> None:  # no-op
        return None

    def json(self) -> Any:
        return self._payload


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:  # type: ignore
        self.calls.append((url, kwargs))
        return FakeResponse({'ok': True})


def test_extract_uses_session_when_provided() -> None:
    sess = FakeSession()
    c = EndpointClient(
        base_url='https://api.example.com',
        endpoints={},
        retry=None,
        session=sess,
    )
    out = c.paginate_url(
        'https://api.example.com/items',
        params=None,
        headers=None,
        timeout=5,
        pagination=None,
    )
    assert out == {'ok': True}
    assert len(sess.calls) == 1
    url, kwargs = sess.calls[0]
    assert url.endswith('/items')
    # ensure timeout propagated
    assert kwargs.get('timeout') == 5


def test_extract_uses_session_factory_when_no_explicit_session() -> None:
    sess = FakeSession()

    def _factory() -> FakeSession:
        return sess

    c = EndpointClient(
        base_url='https://api.example.com',
        endpoints={},
        retry=None,
        session_factory=_factory,
    )
    out = c.paginate_url(
        'https://api.example.com/items',
        params={'a': '1'},
        headers={'x': 'y'},
        timeout=3,
        pagination=None,
    )
    assert out == {'ok': True}
    assert len(sess.calls) == 1
    url, kwargs = sess.calls[0]
    assert url.endswith('/items')
    assert kwargs.get('params') == {'a': '1'}
    assert kwargs.get('headers') == {'x': 'y'}
    assert kwargs.get('timeout') == 3
