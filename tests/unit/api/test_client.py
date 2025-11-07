"""
tests.unit.api.test_client
==========================

Verify that when a client is constructed with a base URL that includes a path
prefix (e.g., an API's effective base URL with base_path), calling
``paginate(endpoint_key, ...)`` builds URLs that include that path.

This is a pure URL-building test; network calls are mocked by patching the
module-level ``_extract`` function used by ``EndpointClient``.
"""
from __future__ import annotations

import types
from typing import Any

import pytest
import requests  # type: ignore

import etlplus.api.client as cmod
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


def test_cursor_pagination_error_includes_page_number(
    monkeypatch: Any,
) -> None:
    """
    When a cursor-paginated request fails, PaginationError includes page.
    """

    client = EndpointClient(
        base_url='https://api.example.com/v1',
        endpoints={'list': '/items'},
    )

    # First page succeeds with next cursor; second raises 500
    calls = {'n': 0}

    def extractor(_stype: str, _url: str, **kw: Any):  # noqa: ARG001
        calls['n'] += 1
        if calls['n'] == 1:
            return {
                'items': [{'i': 1}],
                'meta': {'next': 'xyz'},
            }
        raise make_http_error(500)

    monkeypatch.setattr('etlplus.api.client._extract', extractor)

    pagination: dict[str, Any] = {
        'type': 'cursor',
        'cursor_param': 'cursor',
        'cursor_path': 'meta.next',
        'page_size': 1,
        'records_path': 'items',
    }

    with pytest.raises(Exception) as ei:
        list(
            client.paginate_iter(
                'list', pagination=pagination,  # type: ignore[arg-type]
            ),
        )

    from etlplus.api.errors import PaginationError

    err = ei.value
    assert isinstance(err, PaginationError)
    assert err.page == 2
    assert err.status == 500


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


def test_paginate_cursor_adds_limit_and_advances_cursor(monkeypatch):
    """Cursor pagination uses limit and advances cursors across pages."""

    # Simulate two pages: cursor 'abc' on first, then none on second
    calls: list[dict[str, Any]] = []

    def fake_extract(kind: str, _url: str, **kwargs: Any):
        assert kind == 'api'
        calls.append(kwargs)
        params = kwargs.get('params') or {}
        if 'cursor' not in params:
            return {'items': [{'i': 1}], 'next': 'abc'}
        return {'items': [{'i': 2}], 'next': None}

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(base_url='https://example.test', endpoints={})
    pagination = {
        'type': 'cursor',
        'cursor_param': 'cursor',
        'cursor_path': 'next',
        'page_size': 10,
        'records_path': 'items',
    }
    data = client.paginate_url(
        'https://example.test/x', None, None, None, pagination,
    )
    assert [r['i'] for r in data] == [1, 2]
    # First call has limit=10 without cursor; second call has cursor='abc'
    first, second = calls
    assert first.get('params', {}).get('limit') == 10
    assert 'cursor' not in (first.get('params') or {})
    assert second.get('params', {}).get('cursor') == 'abc'
    assert second.get('params', {}).get('limit') == 10


def test_paginate_page_short_batch(monkeypatch):
    """Page pagination stops when last batch shorter than page_size."""

    def fake_extract(kind: str, _url: str, **kwargs: Any):
        assert kind == 'api'
        params = kwargs.get('params') or {}
        page = int(params.get('page', 1))
    # page_size=2 via config; emulate two pages then short batch
        if page == 1:
            return [{'id': 1}, {'id': 2}]
        if page == 2:
            return [{'id': 3}]
        return []

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    url = 'https://example.test/api'
    pagination = {
        'type': 'page',
        'page_param': 'page',
        'size_param': 'per_page',
        'start_page': 1,
        'page_size': 2,
        'records_path': None,
    }
    client = EndpointClient(base_url='https://example.test', endpoints={})
    data = client.paginate_url(url, None, None, 5, pagination)
    assert [r['id'] for r in data] == [1, 2, 3]


def test_paginate_page_max_records_cap(monkeypatch):
    """Respects max_records cap, truncating final batch as needed."""

    def fake_extract(kind: str, _url: str, **kwargs: Any):
        assert kind == 'api'
        params = kwargs.get('params') or {}
        page = int(params.get('page', 1))
        # Each page returns 3 records to force truncation
        return [{'p': page, 'i': i} for i in range(3)]

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(base_url='https://example.test', endpoints={})
    pagination = {
        'type': 'page',
        'page_param': 'page',
        'size_param': 'per_page',
        'start_page': 1,
        'page_size': 3,
        'max_records': 5,  # should truncate second page (total would be 6)
    }
    data = client.paginate_url(
        'https://example.test/x', None, None, None, pagination,
    )
    assert len(data) == 5
    assert all('p' in r for r in data)


def test_paginate_page_size_normalization(monkeypatch):
    """Normalize page_size < 1 to 1 ensuring progress and termination."""

    def fake_extract(kind: str, _url: str, **kwargs: Any):
        assert kind == 'api'
        params = kwargs.get('params') or {}
        page = int(params.get('page', 1))
        # Return single record; page_size gets normalized to 1
        return [{'id': page}]

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(base_url='https://example.test', endpoints={})
    pagination = {
        'type': 'page',
        'page_param': 'page',
        'size_param': 'per_page',
        'start_page': 1,
        'page_size': 0,  # invalid, should normalize
        'max_pages': 3,
    }
    data = client.paginate_url(
        'https://example.test/x', None, None, None, pagination,
    )
    assert [r['id'] for r in data] == [1, 2, 3]


def test_paginate_unknown_type_returns_raw(monkeypatch):
    """
    Unknown pagination type bypasses iterator and returns raw JSON payload.
    """

    def fake_extract(kind: str, _url: str, **_kwargs: Any):
        assert kind == 'api'
        return {'foo': 'bar'}

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(base_url='https://example.test', endpoints={})
    # use bogus type 'weird' to exercise non-iter-path in paginate_url
    out = client.paginate_url(
        'https://example.test/x', None, None, None, {'type': 'weird'},
    )
    assert out == {'foo': 'bar'}


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


@pytest.mark.parametrize(
    'base_url,base_path,endpoint,expected',
    [
        (
            'https://api.example.com',
            'v2',
            'items',
            'https://api.example.com/v2/items',
        ),
        (
            'https://api.example.com',
            '/v2',
            '/items',
            'https://api.example.com/v2/items',
        ),
        (
            'https://api.example.com/api',
            'v1',
            '/items',
            'https://api.example.com/api/v1/items',
        ),
        # Note: trailing slashes on base_url/base_path not normalized by client
    ],
)
def test_url_composition(monkeypatch, base_url, base_path, endpoint, expected):
    """
    URL composition should honor base_url path + base_path variants.
    """
    captured: list[str] = []

    def fake_extract(kind: str, url: str, **_kwargs: Any):
        assert kind == 'api'
        captured.append(url)
        return {'ok': True}

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(
        base_url=base_url,
        endpoints={'list': endpoint},
        base_path=base_path,
    )
    out = client.paginate('list', pagination=None)
    assert out == {'ok': True}
    assert captured == [expected]


def test_url_query_merging_and_path_encoding(monkeypatch):
    """
    Merges base_url query with query_parameters and encodes path.
    """

    captured: list[str] = []

    def fake_extract(kind: str, url: str, **_kwargs: Any):
        assert kind == 'api'
        captured.append(url)
        return {'ok': True}

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(
        base_url='https://api.example.com/v1?existing=a&dup=1',
        endpoints={'item': '/users/{id}'},
    )

    out = client.paginate(
        'item',
        path_parameters={'id': 'A/B C'},  # slash encoded; space -> +
        query_parameters={'q': 'x y', 'dup': '2'},
        pagination=None,
    )
    assert out == {'ok': True}
    # Path encoded; queries merged with duplicates preserved (doseq)
    expected = (
        'https://api.example.com/v1/users/A%2FB%20C?'
        'existing=a&dup=1&q=x+y&dup=2'
    )
    assert captured[0] == expected


def test_paginate_cursor_with_start_cursor(monkeypatch):
    """
    First call includes provided start_cursor; then advances via cursor_path.
    """

    calls: list[dict[str, Any]] = []

    def fake_extract(kind: str, _url: str, **kwargs: Any):
        assert kind == 'api'
        calls.append(kwargs)
        params = kwargs.get('params') or {}
        if params.get('cursor') == 'seed':
            return {'items': [{'i': 1}], 'next': 'abc'}
        return {'items': [{'i': 2}], 'next': None}

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(base_url='https://example.test', endpoints={})
    pagination = {
        'type': 'cursor',
        'cursor_param': 'cursor',
        'cursor_path': 'next',
        'page_size': 2,
        'records_path': 'items',
        'start_cursor': 'seed',
    }
    data = client.paginate_url(
        'https://example.test/x', None, None, None, pagination,
    )
    assert [r['i'] for r in data] == [1, 2]
    first, second = calls
    assert first.get('params', {}).get('cursor') == 'seed'
    assert second.get('params', {}).get('cursor') == 'abc'


def test_url_query_param_ordering(monkeypatch):
    """
    Existing base_url params should precede appended query_parameters (doseq).
    """

    captured: list[str] = []

    def fake_extract(kind: str, url: str, **_kwargs: Any):
        assert kind == 'api'
        captured.append(url)
        return {'ok': True}

    monkeypatch.setattr(cmod, '_extract', fake_extract)

    client = EndpointClient(
        base_url='https://api.example.com/v1?z=9&dup=1',
        endpoints={'e': '/ep'},
    )
    client.paginate(
        'e',
        query_parameters={'a': '1', 'dup': '2'},
        pagination=None,
    )
    assert captured[0] == 'https://api.example.com/v1/ep?z=9&dup=1&a=1&dup=2'
