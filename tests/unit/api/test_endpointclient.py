"""
ETLPlus API Error Tests
=======================

Unit tests for the ETLPlus API error handling.

Notes
-----
These tests cover the wrapping and propagation of API errors.
"""
from __future__ import annotations

import types
from typing import Any

import pytest
import requests  # type: ignore

from etlplus.api import EndpointClient
from tests.unit.api.test_mocks import MockSession


def make_http_error(status: int) -> requests.HTTPError:
    err = requests.HTTPError(f"HTTP {status}")
    # Attach a response-like object that exposes status_code
    resp = requests.Response()
    resp.status_code = status
    # type: ignore[attr-defined]
    err.response = resp
    return err


def test_auth_error_wrapping_on_single_attempt(monkeypatch: Any) -> None:
    client = EndpointClient(
        base_url='https://api.example.com/v1',
        endpoints={'x': '/x'},
    )

    def boom(_stype: str, url: str, **kw: Any):  # noqa: ARG001
        raise make_http_error(401)

    # Patch the module-level _extract symbol imported in client
    monkeypatch.setattr('etlplus.api.client._extract', boom)

    with pytest.raises(Exception) as ei:
        client.paginate_url(
            'https://api.example.com/v1/x', None, None, None, None,
        )

    err = ei.value
    from etlplus.api.errors import ApiAuthError

    assert isinstance(err, ApiAuthError)
    assert err.status == 401
    assert err.attempts == 1
    assert err.retried is False
    assert err.retry_policy is None


def test_context_manager_closes_factory_session(monkeypatch: Any) -> None:
    def fake_extract(_stype: str, _url: str, **_kw: Any):  # noqa: ARG001
        return {'ok': True}

    import etlplus.api.client as cmod

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    sess = MockSession()

    def factory() -> MockSession:
        return sess

    c = EndpointClient(
        base_url='https://api.example.com',
        endpoints={},
        session_factory=factory,
    )
    with c:
        out = c.paginate_url(
            'https://api.example.com/items', None, None, None, None,
        )
        assert out == {'ok': True}
    assert sess.closed is True


def test_context_manager_creates_and_closes_default_session(
    monkeypatch: Any,
) -> None:
    # Patch extract to avoid network and capture params
    def fake_extract(_stype: str, _url: str, **_kw: Any):  # noqa: ARG001
        return {'ok': True}

    import etlplus.api.client as cmod

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    # Substitute requests.Session with our FakeSession to observe close()
    created: dict[str, MockSession] = {}

    def ctor() -> MockSession:
        s = MockSession()
        created['s'] = s
        return s

    monkeypatch.setattr(cmod.requests, 'Session', ctor)

    c = EndpointClient(base_url='https://api.example.com', endpoints={})
    with c:
        out = c.paginate_url(
            'https://api.example.com/items', None, None, None, None,
        )
        assert out == {'ok': True}
    # After context exit, the created session should be closed
    assert created['s'].closed is True


def test_context_manager_does_not_close_external_session(
    monkeypatch: Any,
) -> None:
    def fake_extract(_stype: str, _url: str, **_kw: Any):  # noqa: ARG001
        return {'ok': True}

    import etlplus.api.client as cmod

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    sess = MockSession()
    c = EndpointClient(
        base_url='https://api.example.com', endpoints={}, session=sess,
    )
    with c:
        out = c.paginate_url(
            'https://api.example.com/items', None, None, None, None,
        )
        assert out == {'ok': True}
    assert sess.closed is False


def test_extract_uses_session_factory_when_no_explicit_session() -> None:
    sess = MockSession()

    def _factory() -> MockSession:
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


def test_extract_uses_session_when_provided() -> None:
    sess = MockSession()
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


def test_pagination_error_includes_page_number(monkeypatch: Any) -> None:
    client = EndpointClient(
        base_url='https://api.example.com/v1',
        endpoints={'list': '/items'},
    )

    page_size = 2

    def extractor(_stype: str, _url: str, **kw: Any):  # noqa: ARG001
        params = kw.get('params') or {}
        page = int(params.get('page', 1))
        size = int(params.get('per_page', page_size))
        if page == 4:
            raise make_http_error(500)
        # Return exactly `size` records to force continue until failure
        return {'items': [{'i': i} for i in range(size)]}

    monkeypatch.setattr('etlplus.api.client._extract', extractor)

    pagination: dict[str, Any] = {
        'type': 'page',
        'page_param': 'page',
        'size_param': 'per_page',
        'start_page': 3,
        'page_size': page_size,
        'records_path': 'items',
    }

    with pytest.raises(Exception) as ei:
        client.paginate(
            'list', pagination=pagination,  # type: ignore[arg-type]
        )

    from etlplus.api.errors import PaginationError

    err = ei.value
    assert isinstance(err, PaginationError)
    assert err.page == 4
    assert err.status == 500


def test_request_error_after_retries_exhausted(monkeypatch: Any) -> None:
    # Retry twice, both 503
    client = EndpointClient(
        base_url='https://api.example.com/v1',
        endpoints={'x': '/x'},
        retry={'max_attempts': 2, 'backoff': 0.0, 'retry_on': [503]},
    )

    attempts = {'n': 0}

    def boom(_stype: str, _url: str, **_kw: Any):  # noqa: ARG001
        attempts['n'] += 1
        raise make_http_error(503)

    monkeypatch.setattr('etlplus.api.client._extract', boom)

    with pytest.raises(Exception) as ei:
        client.paginate_url(
            'https://api.example.com/v1/x', None, None, None, None,
        )

    from etlplus.api.errors import ApiRequestError

    err = ei.value
    assert isinstance(err, ApiRequestError)
    assert err.status == 503
    assert err.attempts == 2  # exhausted
    assert err.retried is True


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


def test_retry_on_network_errors(monkeypatch) -> None:
    """
    When retry_network_errors=True, the client should retry on timeouts and
    connection errors using jittered backoff, then succeed.
    """

    sleeps: list[float] = []

    # Capture sleep calls
    monkeypatch.setattr(
        EndpointClient,
        'apply_sleep',
        staticmethod(lambda s, *, sleeper=None: sleeps.append(s)),
        raising=False,
    )

    # Deterministic jitter
    import etlplus.api.client as client_mod

    jitter_vals = iter([0.12, 0.18])
    monkeypatch.setattr(
        client_mod.random,
        'uniform',
        lambda a, b: next(jitter_vals),
    )

    # Simulate Timeout then ConnectionError then success
    attempts = {'n': 0}

    def _fake_extract(_stype: str, _url: str, **_kw: Any) -> Any:
        attempts['n'] += 1
        if attempts['n'] == 1:
            raise requests.Timeout('slow')
        if attempts['n'] == 2:
            raise requests.ConnectionError('reset')
        return {'ok': True}

    monkeypatch.setattr(client_mod, '_extract', _fake_extract)

    c = EndpointClient(
        base_url='https://api.example.com',
        endpoints={},
        retry={'max_attempts': 4, 'backoff': 0.5},
        retry_network_errors=True,
    )

    out = c._extract_with_retry('https://api.example.com/items')
    assert out == {'ok': True}

    # Should have slept twice (after 2 failures)
    assert sleeps == [0.12, 0.18]
    assert attempts['n'] == 3
