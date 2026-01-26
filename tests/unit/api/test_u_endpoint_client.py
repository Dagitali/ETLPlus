"""
:mod:`tests.unit.api.test_u_endpoint_client` module.

Unit tests for :mod:`etlplus.api.endpoint_client`.

Notes
-----
- Preserves path prefixes in composed URLs.
- Mocks network calls via patched extract helpers.
- Includes optional Hypothesis-based property tests when available.
"""

from __future__ import annotations

import types
import urllib.parse as urlparse
from collections.abc import Callable
from collections.abc import Sequence
from functools import partial
from typing import Any
from typing import cast

import pytest
import requests  # type: ignore[import]

import etlplus.api.endpoint_client as ec_module
import etlplus.api.request_manager as rm_module
from etlplus.api import CursorPaginationConfigMap
from etlplus.api import EndpointClient
from etlplus.api import PagePaginationConfigMap
from etlplus.api import PaginationType
from etlplus.api import RequestOptions
from etlplus.api import RetryPolicy
from etlplus.api import errors as api_errors
from tests.unit.api.test_u_mocks import MockSession

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

EXAMPLE_BASE_URL = 'https://example.test'

type CursorConfigFactory = Callable[..., CursorPaginationConfigMap]
type StubRequestManager = Callable[
    [Sequence[dict[str, Any]]],
    list[dict[str, Any]],
]

# Optional Hypothesis import with safe stubs when missing.
try:  # pragma: no try
    from hypothesis import given  # type: ignore[import-not-found]
    from hypothesis import strategies as st  # type: ignore[import-not-found]

    _HYP_AVAILABLE = True
except ImportError:  # pragma: no cover
    _HYP_AVAILABLE = False

    def given(*_a, **_k):  # type: ignore[unused-ignore]
        """No-op decorator when Hypothesis is unavailable."""

        def _wrap(fn):
            return pytest.mark.skip(reason='needs hypothesis')(fn)

        return _wrap

    class _Strategy:  # minimal chainable strategy stub
        def filter(self, *_a, **_k):  # pragma: no cover
            """No-op filter when Hypothesis is unavailable."""
            return self

    class _DummyStrategies:
        def text(self, *_a, **_k):  # pragma: no cover
            """No-op text strategy when Hypothesis is unavailable."""
            return _Strategy()

        def characters(self, *_a, **_k):  # pragma: no cover
            """No-op characters strategy when Hypothesis is unavailable."""
            return _Strategy()

        def dictionaries(self, *_a, **_k):  # pragma: no cover
            """No-op dictionaries strategy when Hypothesis is unavailable."""
            return _Strategy()

    st = _DummyStrategies()  # type: ignore[assignment]


def _ascii_no_amp_eq() -> Any:
    """
    Returns a Hypothesis strategy for ASCII text excluding '&' and '='.

    Returns
    -------
    Any
        Hypothesis strategy for text.
    """
    alpha = st.characters(min_codepoint=32, max_codepoint=126).filter(
        lambda ch: ch not in ['&', '='],
    )
    return st.text(alphabet=alpha, min_size=0, max_size=12)


def _page_responder(
    pages: Sequence[Sequence[dict[str, Any]]],
) -> Callable[..., list[dict[str, Any]]]:
    """
    Build a deterministic RequestManager stub for page pagination tests.

    Parameters
    ----------
    pages : Sequence[Sequence[dict[str, Any]]]
        1-indexed payloads returned for page requests.

    Returns
    -------
    Callable[..., list[dict[str, Any]]]
        Handler compatible with ``patch_request_once``.
    """
    # pylint: disable=unused-argument

    def _handler(
        self: EndpointClient,
        method: str,
        _url: str,
        *,
        session: Any,
        timeout: Any,
        **kwargs: dict[str, Any],
    ) -> list[dict[str, Any]]:
        assert method == 'GET'
        params = kwargs.get('params') or {}
        page = int(params.get('page', 1))
        if 1 <= page <= len(pages):
            return list(pages[page - 1])
        return []

    return _handler


def _stub_request_manager(
    monkeypatch: pytest.MonkeyPatch,
    responses: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Patch ``RequestManager.request_once`` with deterministic responses.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest fixture for attribute patching.
    responses : Sequence[dict[str, Any]]
        Ordered responses returned per invocation. The last payload is reused
        if the stub is called more times than there are entries.

    Returns
    -------
    list[dict[str, Any]]
        Mutable call log containing ``url`` and ``kwargs`` for each request.

    Raises
    ------
    ValueError
        If *responses* is empty.
    """
    # pylint: disable=unused-argument

    if not responses:
        msg = 'responses must contain at least one payload'
        raise ValueError(msg)

    calls: list[dict[str, Any]] = []

    def _fake_request(
        self: rm_module.RequestManager,  # noqa: D401
        method: str,
        url: str,
        *,
        session: Any,
        timeout: Any,
        request_callable: Callable[..., Any] | None = None,
        **kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        idx = min(len(calls), len(responses) - 1)
        calls.append({'method': method, 'url': url, 'kwargs': kwargs})
        return responses[idx]

    monkeypatch.setattr(
        rm_module.RequestManager,
        'request_once',
        _fake_request,
    )

    return calls


def make_http_error(status: int) -> requests.HTTPError:
    """
    Create a requests.HTTPError with attached response object.

    Parameters
    ----------
    status : int
        HTTP status code to attach.

    Returns
    -------
    requests.HTTPError
        HTTPError with attached response.
    """
    err = requests.HTTPError(f'HTTP {status}')
    resp = requests.Response()
    resp.status_code = status
    err.response = resp  # type: ignore[attr-defined]

    return err


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='stub_request_manager')
def stub_request_manager_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[Sequence[dict[str, Any]]], list[dict[str, Any]]]:
    """
    Provide a callable that stubs ``RequestManager.request_once``.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Fixture used to patch request helpers.

    Returns
    -------
    Callable[[Sequence[dict[str, Any]]], list[dict[str, Any]]]
        Factory that applies :func:`_stub_request_manager` with captured
        responses.
    """

    return partial(_stub_request_manager, monkeypatch)


@pytest.fixture(name='patch_request_once')
def patch_request_once_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Provide a helper to patch ``RequestManager.request_once``."""

    def _apply(handler: Callable[..., Any]) -> Callable[..., Any]:
        monkeypatch.setattr(
            rm_module.RequestManager,
            'request_once',
            handler,
        )
        return handler

    return _apply


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestContextManager:
    """Unit test suite for :class:`EndpointClient`."""

    def test_closes_factory_session(
        self,
        base_url: str,
        mock_session: MockSession,
        client_factory: Callable[..., EndpointClient],
        request_once_stub: dict[str, Any],
    ) -> None:
        """
        Test that :class:`EndpointClient` closes a session created by a
        factory.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        mock_session : MockSession
            Mocked session object.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        request_once_stub : dict[str, Any]
            Captures calls to the patched HTTP helper.
        """
        sess = mock_session
        client = client_factory(
            endpoints={},
            session_factory=lambda: sess,
        )
        with client:
            out = client.paginate_url(f'{base_url}/items', None)
            assert out == {'ok': True}
        assert sess.closed is True
        assert request_once_stub['urls'] == [f'{base_url}/items']

    def test_creates_and_closes_default_session(
        self,
        base_url: str,
        monkeypatch: pytest.MonkeyPatch,
        client_factory: Callable[..., EndpointClient],
        request_once_stub: dict[str, Any],
    ) -> None:
        """
        Test that :class:`EndpointClient` creates and closes a default session.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        request_once_stub : dict[str, Any]
            Captures calls to the patched HTTP helper.
        """

        # Substitute Session with MockSession to observe close()
        created: dict[str, MockSession] = {}

        def ctor() -> MockSession:
            s = MockSession()
            created['s'] = s
            return s

        # Patch extract to avoid network and capture params.
        monkeypatch.setattr(ec_module.requests, 'Session', ctor)

        client = client_factory(endpoints={})
        with client:
            out = client.paginate_url(f'{base_url}/items', None)
            assert out == {'ok': True}

        # After context exit, the created session should be closed.
        assert created['s'].closed is True
        assert request_once_stub['urls'] == [f'{base_url}/items']

    def test_does_not_close_external_session(
        self,
        base_url: str,
        mock_session: MockSession,
        client_factory: Callable[..., EndpointClient],
        request_once_stub: dict[str, Any],
    ) -> None:
        """
        Test that :class:`EndpointClient` does not close an externally provided
        session.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        mock_session : MockSession
            Mocked session object.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        request_once_stub : dict[str, Any]
            Captures calls to the patched HTTP helper.
        """
        sess = mock_session
        client = client_factory(
            endpoints={},
            session=sess,
        )
        with client:
            out = client.paginate_url(f'{base_url}/items', None)
            assert out == {'ok': True}
        assert sess.closed is False
        assert request_once_stub['urls'] == [f'{base_url}/items']


@pytest.mark.unit
class TestCursorPagination:
    """Unit test suite for :class:`EndpointClient`."""

    @pytest.mark.parametrize(
        'raw_page_size,expected_limit',
        [(-1, 1), ('not-a-number', EndpointClient.DEFAULT_PAGE_SIZE)],
    )
    def test_page_size_normalizes(
        self,
        cursor_cfg: CursorConfigFactory,
        client_factory: Callable[..., EndpointClient],
        stub_request_manager: StubRequestManager,
        raw_page_size: Any,
        expected_limit: int,
    ) -> None:
        """
        Test that page_size is normalized to a valid integer.

        Parameters
        ----------
        cursor_cfg : CursorConfigFactory
            Factory for cursor pagination config.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        stub_request_manager : StubRequestManager
            Fixture that patches the underlying :class:`RequestManager`.
        raw_page_size : Any
            Raw page size input.
        expected_limit : int
            Expected normalized limit.
        """
        calls = stub_request_manager(
            [{'items': [{'i': 1}], 'next': None}],
        )

        client = client_factory(base_url=EXAMPLE_BASE_URL, endpoints={})
        cfg = cursor_cfg(
            cursor_param='cursor',
            cursor_path='next',
            page_size=raw_page_size,
            records_path='items',
        )
        out = client.paginate_url(f'{EXAMPLE_BASE_URL}/x', cfg)
        assert isinstance(out, list)

        # mypy treats list element as Any due to external library response
        # type.
        items = [cast(dict, r)['i'] for r in out]  # type: ignore[index]
        assert items == [1]
        params = calls[0]['kwargs'].get('params', {})
        assert params.get('limit') == expected_limit


@pytest.mark.unit
class TestRequestOptionIntegration:
    """Tests covering RequestOptions propagation across helpers."""

    def test_paginate_url_uses_request_snapshot(
        self,
        base_url: str,
        monkeypatch: pytest.MonkeyPatch,
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """Non-paginated calls honor explicitly supplied RequestOptions."""

        client = client_factory(endpoints={})

        captured: dict[str, Any] = {}

        def fake_get(
            _client: EndpointClient,
            url: str,
            **kwargs: Any,
        ) -> dict[str, bool]:
            captured['url'] = url
            captured['kwargs'] = kwargs
            return {'ok': True}

        monkeypatch.setattr(EndpointClient, 'get', fake_get)

        seed = RequestOptions(
            params={'seed': '1'},
            headers={'X-Seed': 'yes'},
            timeout=4.5,
        )
        out = client.paginate_url(
            f'{base_url}/items',
            None,
            request=seed,
        )

        assert out == {'ok': True}
        assert captured['url'] == f'{base_url}/items'
        assert captured['kwargs']['params'] == {'seed': '1'}
        assert captured['kwargs']['headers'] == {'X-Seed': 'yes'}
        assert captured['kwargs']['timeout'] == 4.5

    def test_paginate_url_iter_overrides_request_params(
        self,
        monkeypatch: pytest.MonkeyPatch,
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """Paginated iterations override RequestOptions params per call."""
        # pylint: disable=unused-argument

        client = client_factory(base_url=EXAMPLE_BASE_URL, endpoints={})
        observed: list[RequestOptions] = []

        def fake_fetch(
            self: EndpointClient,
            _url: str,
            request: RequestOptions,
            page: int | None,
        ) -> dict[str, Any]:
            observed.append(request)
            return {'items': []}

        monkeypatch.setattr(EndpointClient, '_fetch_page', fake_fetch)

        list(
            client.paginate_url_iter(
                f'{EXAMPLE_BASE_URL}/items',
                {
                    'type': PaginationType.PAGE,
                    'records_path': 'items',
                    'page_size': 1,
                },
                request=RequestOptions(
                    params={'seed': 1},
                    headers={'X-Seed': 'yes'},
                    timeout=2.0,
                ),
            ),
        )

        assert observed
        first = observed[0]
        assert first.params == {'seed': 1, 'page': 1, 'per_page': 1}
        assert first.headers == {'X-Seed': 'yes'}
        assert first.timeout == 2.0

    def test_adds_limit_and_advances_cursor(
        self,
        cursor_cfg: CursorConfigFactory,
        client_factory: Callable[..., EndpointClient],
        stub_request_manager: StubRequestManager,
    ) -> None:
        """
        Test that limit is added and cursor advances correctly.

        Parameters
        ----------
        cursor_cfg : CursorConfigFactory
            Factory for cursor pagination config.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        stub_request_manager : StubRequestManager
            Fixture that patches :class:`RequestManager` responses.
        """
        calls = stub_request_manager(
            [
                {'items': [{'i': 1}], 'next': 'abc'},
                {'items': [{'i': 2}], 'next': None},
            ],
        )
        client = client_factory(base_url=EXAMPLE_BASE_URL, endpoints={})
        cfg = cursor_cfg(
            cursor_param='cursor',
            cursor_path='next',
            page_size=10,
            records_path='items',
        )
        data = client.paginate_url(f'{EXAMPLE_BASE_URL}/x', cfg)
        assert isinstance(data, list)
        assert len(calls) >= 2
        values = [cast(dict, r)['i'] for r in data]  # type: ignore[index]
        assert values == [1, 2]
        first = calls[0]['kwargs']
        second = calls[1]['kwargs']
        assert first.get('params', {}).get('limit') == 10
        assert 'cursor' not in (first.get('params') or {})
        assert second.get('params', {}).get('cursor') == 'abc'
        assert second.get('params', {}).get('limit') == 10

    def test_error_includes_page_number(
        self,
        base_url: str,
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        cursor_cfg: CursorConfigFactory,
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that :class:`PaginationError` includes the page number on
        failure.

        When a cursor-paginated request fails, :class:`PaginationError`
        includes page.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper that patches the request helper for deterministic failures.
        cursor_cfg : CursorConfigFactory
            Factory for cursor pagination config.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        client = client_factory(
            base_url=f'{base_url}/v1',
            endpoints={'list': '/items'},
        )
        # pylint: disable=unused-argument

        # First page succeeds with next cursor; second raises 500.
        calls = {'n': 0}

        def extractor(
            self: EndpointClient,
            method: str,
            _url: str,
            *,
            session: Any,
            timeout: Any,
            **kwargs: dict[str, Any],
        ) -> dict[str, Any]:
            assert method == 'GET'
            calls['n'] += 1
            if calls['n'] == 1:
                return {
                    'items': [{'i': 1}],
                    'meta': {'next': 'xyz'},
                }
            raise make_http_error(500)

        patch_request_once(extractor)

        cfg = cursor_cfg(
            cursor_param='cursor',
            cursor_path='meta.next',
            page_size=1,
            records_path='items',
        )

        with pytest.raises(api_errors.PaginationError) as ei:
            list(
                client.paginate_iter('list', pagination=cfg),
            )
        assert ei.value.page == 2 and ei.value.status == 500

    def test_rate_limit_overrides_adjust_sleep(
        self,
        base_url: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that per-call overrides influence paginator pacing.

        When ``rate_limit_overrides`` are provided to ``paginate_url_iter``,
        the computed ``sleep_seconds`` reflects the overrides.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        """
        # pylint: disable=unused-argument

        captured: dict[str, Any] = {}

        def fake_from_config(
            cls: type[EndpointClient],
            config: CursorPaginationConfigMap | PagePaginationConfigMap,
            *,
            fetch: Callable[..., Any],
            rate_limiter: Any,
        ) -> Any:
            captured['rate_limiter'] = rate_limiter
            captured['sleep_seconds'] = (
                getattr(rate_limiter, 'sleep_seconds', 0.0)
                if rate_limiter is not None
                else 0.0
            )

            class _StubPaginator:
                """Stub paginator yielding a single page."""

                def paginate_iter(self, *_args: Any, **_kwargs: Any):
                    """Yield a single stub record."""
                    yield {'id': 1}

            return _StubPaginator()

        monkeypatch.setattr(
            ec_module.Paginator,
            'from_config',
            classmethod(fake_from_config),
        )

        client = EndpointClient(
            base_url=base_url,
            endpoints={'items': '/items'},
            rate_limit={'max_per_sec': 2},
        )

        # pg: PagePaginationConfigMap = {'type': 'page'}
        pg: PagePaginationConfigMap = {'type': PaginationType.PAGE}

        out = list(
            client.paginate_url_iter(
                f'{base_url}/items',
                pagination=pg,
                rate_limit_overrides={'max_per_sec': 4},
            ),
        )

        assert out == [{'id': 1}]
        assert captured['sleep_seconds'] == pytest.approx(0.25)
        limiter = captured['rate_limiter']
        assert limiter is not None
        assert getattr(limiter, 'sleep_seconds', 0.0) == pytest.approx(0.25)

    def test_retry_backoff_sleeps(
        self,
        cursor_cfg: CursorConfigFactory,
        capture_sleeps: list[float],
        jitter: Callable[[list[float]], list[float]],
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that cursor pagination applies retry backoff sleep on failure.

        Cursor pagination applies retry backoff sleep on failure.

        Parameters
        ----------
        cursor_cfg : CursorConfigFactory
            Factory for cursor pagination config.
        capture_sleeps : list[float]
            List to capture sleep durations.
        jitter : Callable[[list[float]], list[float]]
            Jitter function for sleep values.
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper that patches the request helper for deterministic retries.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        # pylint: disable=unused-argument

        jitter([0.05])

        attempts = {'n': 0}

        def fake_request(
            self: EndpointClient,
            method: str,
            _url: str,
            *,
            session: Any,
            timeout: Any,
            **_k: Any,
        ) -> dict[str, Any]:
            assert method == 'GET'
            attempts['n'] += 1
            if attempts['n'] == 1:
                err = requests.HTTPError('boom')
                err.response = types.SimpleNamespace(status_code=503)
                raise err
            return {'items': [{'i': 1}], 'next': None}

        patch_request_once(fake_request)
        client = client_factory(
            base_url=EXAMPLE_BASE_URL,
            endpoints={},
            retry={'max_attempts': 2, 'backoff': 0.5, 'retry_on': [503]},
        )
        cfg = cursor_cfg(
            cursor_param='cursor',
            cursor_path='next',
            page_size=2,
            records_path='items',
        )

        out = client.paginate_url(f'{EXAMPLE_BASE_URL}/x', cfg)
        assert out == [{'i': 1}]

        # One sleep from the single retry attempt.
        assert len(capture_sleeps) == 1
        assert abs(capture_sleeps[0] - 0.05) < 1e-6
        assert attempts['n'] == 2


@pytest.mark.unit
class TestErrors:
    """Unit test suite for :class:`ApiAuthError`."""

    def test_auth_error_wrapping_on_single_attempt(
        self,
        base_url: str,
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that :class:`ApiAuthError` is raised and wrapped on a single
        attempt.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper used to patch the request helper.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        # pylint: disable=unused-argument

        client = client_factory(
            base_url=base_url,
            endpoints={'x': '/x'},
        )

        def boom(
            self: EndpointClient,
            method: str,
            url: str,
            *,
            session: Any,
            timeout: Any,
            **_kwargs: dict[str, Any],
        ) -> dict[str, Any]:
            assert method == 'GET'
            raise make_http_error(401)

        patch_request_once(boom)
        with pytest.raises(api_errors.ApiAuthError) as ei:
            client.paginate_url(f'{base_url}/x', None)
        err = ei.value
        assert err.status == 401
        assert err.attempts == 1
        assert err.retried is False
        assert err.retry_policy is None


@pytest.mark.unit
class TestOffsetPagination:
    """
    Unit test suite for offset pagination in :class:`EndpointClient`.

    Tests offset-based pagination logic, including correct offset stepping,
    limit handling, and record truncation.
    """

    def test_offset_pagination_behaves_like_offset(
        self,
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that offset pagination behaves as expected.

        Parameters
        ----------
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper that patches request execution for deterministic pages.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        # pylint: disable=unused-argument

        calls: list[dict[str, Any]] = []

        def fake_request(
            self: EndpointClient,
            method: str,
            _url: str,
            *,
            session: Any,
            timeout: Any,
            **kwargs: dict[str, Any],
        ) -> list[dict[str, int]]:
            assert method == 'GET'
            calls.append(kwargs)
            params = kwargs.get('params') or {}
            off = int(params.get('offset', 0))
            limit = int(params.get('limit', 2))
            # return exactly `limit` items until offset reaches 4
            if off >= 4:
                return []
            return [{'i': i} for i in range(off, off + limit)]

        patch_request_once(fake_request)

        client = client_factory(
            base_url=EXAMPLE_BASE_URL,
            endpoints={},
        )
        cfg = cast(
            PagePaginationConfigMap,
            {
                'type': 'offset',
                'page_param': 'offset',
                'size_param': 'limit',
                'start_page': 0,
                'page_size': 2,
                'max_records': 3,
            },
        )

        data = client.paginate_url(f'{EXAMPLE_BASE_URL}/api', cfg)

        # Expected behavior: collects up to max_records using offset stepping.
        assert [r['i'] for r in cast(list[dict[str, int]], data)] == [0, 1, 2]


@pytest.mark.unit
class TestPagePagination:
    """
    Unit test suite for page-based pagination in :class:`EndpointClient`.

    Tests page-based pagination logic, including batch handling, page size
    normalization, error propagation, and query parameter merging.
    """

    def test_stops_on_short_final_batch(
        self,
        page_cfg: Callable[..., PagePaginationConfigMap],
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that pagination stops on a short final batch.

        Parameters
        ----------
        page_cfg : Callable[..., PagePaginationConfigMap]
            Factory for page pagination config.
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper that patches the request helper.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        patch_request_once(
            _page_responder(
                (
                    ({'id': 1}, {'id': 2}),
                    ({'id': 3},),
                ),
            ),
        )
        client = client_factory(
            base_url=EXAMPLE_BASE_URL,
            endpoints={},
        )
        cfg = page_cfg(
            page_param='page',
            size_param='per_page',
            start_page=1,
            page_size=2,
        )
        data = client.paginate_url(f'{EXAMPLE_BASE_URL}/api', cfg)
        assert isinstance(data, list)
        ids = [cast(dict, r)['id'] for r in data]  # type: ignore[index]
        assert ids == [1, 2, 3]

    def test_max_records_cap(
        self,
        page_cfg: Callable[..., PagePaginationConfigMap],
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that max_records parameter truncates results as expected.

        Parameters
        ----------
        page_cfg : Callable[..., PagePaginationConfigMap]
            Factory for page pagination config.
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper that patches the request helper for fixed responses.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        patch_request_once(
            _page_responder(
                tuple(
                    tuple({'p': page, 'i': i} for i in range(3))
                    for page in range(1, 5)
                ),
            ),
        )

        client = client_factory(
            base_url=EXAMPLE_BASE_URL,
            endpoints={},
        )
        cfg = page_cfg(
            page_param='page',
            size_param='per_page',
            start_page=1,
            page_size=3,
            max_records=5,  # Should truncate 2nd page (total would be 6).
        )
        data = client.paginate_url(f'{EXAMPLE_BASE_URL}/x', cfg)
        assert len(data) == 5
        assert all('p' in r for r in data)

    def test_page_size_normalization(
        self,
        page_cfg: Callable[..., PagePaginationConfigMap],
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that page_size is normalized to 1 if set to 0.

        Parameters
        ----------
        page_cfg : Callable[..., PagePaginationConfigMap]
            Factory for page pagination config.
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper that patches request execution.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        patch_request_once(
            _page_responder(
                (
                    ({'id': 1},),
                    ({'id': 2},),
                    ({'id': 3},),
                ),
            ),
        )

        client = client_factory(
            base_url=EXAMPLE_BASE_URL,
            endpoints={},
        )
        cfg = page_cfg(
            page_param='page',
            size_param='per_page',
            start_page=1,
            page_size=0,
            max_pages=3,
        )
        data = client.paginate_url(f'{EXAMPLE_BASE_URL}/x', cfg)
        assert isinstance(data, list)
        ids = [cast(dict, r)['id'] for r in data]  # type: ignore[index]
        assert ids == [1, 2, 3]

    def test_error_includes_page_number(
        self,
        base_url: str,
        page_cfg: Callable[..., PagePaginationConfigMap],
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that :class:`PaginationError` includes the page number on failure.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        page_cfg : Callable[..., PagePaginationConfigMap]
            Factory for page pagination config.
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper that patches the request helper.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        client = client_factory(
            base_url=base_url,
            endpoints={'list': '/items'},
        )
        # pylint: disable=unused-argument

        page_size = 2

        def extractor(
            self: EndpointClient,
            method: str,
            _url: str,
            *,
            session: Any,
            timeout: Any,
            **kwargs: dict[str, Any],
        ) -> dict[str, Any]:
            assert method == 'GET'
            params = kwargs.get('params') or {}
            page = int(params.get('page', 1))
            size = int(params.get('per_page', page_size))
            if page == 4:
                raise make_http_error(500)
            return {'items': [{'i': i} for i in range(size)]}

        # Return exactly `size` records to force continue until failure.
        patch_request_once(extractor)
        cfg = page_cfg(
            page_param='page',
            size_param='per_page',
            start_page=3,
            page_size=page_size,
            records_path='items',
        )

        with pytest.raises(api_errors.PaginationError) as ei:
            client.paginate('list', pagination=cfg)
        # The paginator reports the iteration count (2) rather than literal
        # page 4.
        assert ei.value.page == 2 and ei.value.status == 500

    def test_unknown_type_returns_raw(
        self,
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that unknown pagination type returns raw output.

        Parameters
        ----------
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper that patches request execution.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        # pylint: disable=unused-argument

        def _raw_response(
            self: EndpointClient,
            method: str,
            _u: str,
            *,
            session: Any,
            timeout: Any,
            **kwargs: Any,
        ) -> dict[str, str] | None:  # noqa: ARG005
            return {'foo': 'bar'} if method == 'GET' else None

        patch_request_once(_raw_response)
        client = client_factory(
            base_url=EXAMPLE_BASE_URL,
            endpoints={},
        )

        out = client.paginate_url(
            f'{EXAMPLE_BASE_URL}/x',
            cast(Any, {'type': 'weird'}),
        )
        assert out == {'foo': 'bar'}


@pytest.mark.unit
class TestRateLimitPrecedence:
    """
    Unit test suite for rate limit precedence in :class:`EndpointClient`.

    Tests explicit sleep_seconds override, rate_limit config precedence, and
    correct sleep duration application during pagination.
    """

    def test_overrides_sleep_seconds_wins(
        self,
        base_url: str,
        capture_sleeps: list[float],
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that explicit sleep_seconds overrides rate_limit config.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        capture_sleeps : list[float]
            List to capture sleep durations.
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper that patches the request helper.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        # :func:`capture_sleeps` fixture already records rate-limiter pacing.
        client = client_factory(
            base_url=base_url,
            endpoints={'list': '/items'},
            rate_limit={'max_per_sec': 2},  # would imply 0.5s if used
        )

        # Patch HTTP request helper to return enough pages for two sleeps.
        calls = {'n': 0}

        def fake_request(
            self: EndpointClient,
            method: str,
            url: str,
            *,
            session: Any,
            timeout: Any,
            **kw: Any,
        ) -> list[dict[str, int]]:  # noqa: D401
            # pylint: disable=unused-argument

            assert method == 'GET'
            calls['n'] += 1
            # Return full page until third call which ends pagination.
            if calls['n'] < 3:
                return [{'i': calls['n']}, {'i': calls['n'] + 10}]
            return []

        patch_request_once(fake_request)

        list(
            client.paginate_iter(
                'list',
                pagination=cast(
                    PagePaginationConfigMap,
                    {
                        'type': 'page',
                        'page_size': 2,
                        'start_page': 1,
                    },
                ),
                sleep_seconds=0.05,  # explicit override should win
            ),
        )

        # We expect exactly two sleeps (between three page fetches) and both
        # should reflect explicit override (0.05) not derived 0.5.
        assert capture_sleeps == [pytest.approx(0.05), pytest.approx(0.05)]


@pytest.mark.unit
class TestRetryLogic:
    """
    Unit test suite for retry logic in :class:`EndpointClient`.

    This suite covers:
    - Retry behavior for request errors
    - Jitter backoff and sleep duration
    - Network error handling and retry policy application
    - Error propagation and attempt counting

    Notes
    -----
    - Uses monkeypatch to simulate failures and control retry attempts.
    - Uses pytest fixtures for retry configuration and sleep capture.
    """

    def test_request_error_after_retries_exhausted(
        self,
        base_url: str,
        retry_cfg: Callable[..., dict[str, Any]],
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that :class:`ApiRequestError` is raised after retries are
        exhausted.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        retry_cfg : Callable[..., dict[str, Any]]
            Factory for retry configuration.
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper used to patch the request helper.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        # pylint: disable=unused-argument

        client = client_factory(
            base_url=base_url,
            endpoints={'x': '/x'},
            retry=cast(
                RetryPolicy,
                retry_cfg(max_attempts=2, backoff=0.0, retry_on=[503]),
            ),
        )
        attempts = {'n': 0}

        def boom(
            self: EndpointClient,
            method: str,
            _url: str,
            *,
            session: Any,
            timeout: Any,
            **_kwargs: dict[str, Any],
        ) -> dict[str, Any]:
            assert method == 'GET'
            attempts['n'] += 1
            raise make_http_error(503)

        patch_request_once(boom)

        with pytest.raises(api_errors.ApiRequestError) as ei:
            client.paginate_url(f'{base_url}/x', None)
        err = ei.value
        assert isinstance(err, api_errors.ApiRequestError)
        assert err.status == 503
        assert err.attempts == 2  # Exhausted
        assert err.retried is True

    def test_full_jitter_backoff(
        self,
        base_url: str,
        capture_sleeps: list[float],
        retry_cfg: Callable[..., dict[str, Any]],
        jitter: Callable[[list[float]], list[float]],
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that full jitter backoff is applied on retries.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        capture_sleeps : list[float]
            List to capture sleep durations.
        retry_cfg : Callable[..., dict[str, Any]]
            Factory for retry configuration.
        jitter : Callable[[list[float]], list[float]]
            Jitter function for sleep values.
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper used to patch the request helper.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        # pylint: disable=unused-argument

        jitter([0.1, 0.2])

        # Patch HTTP helper to fail with 503 twice, then succeed.
        attempts = {'n': 0}

        def _fake_request(
            self: EndpointClient,
            method: str,
            _url: str,
            *,
            session: Any,
            timeout: Any,
            **_kwargs: dict[str, Any],
        ) -> dict[str, Any]:
            assert method == 'GET'
            attempts['n'] += 1
            if attempts['n'] < 3:
                err = requests.HTTPError('boom')
                err.response = types.SimpleNamespace(status_code=503)
                raise err
            return {'ok': True}

        patch_request_once(_fake_request)

        client = client_factory(
            base_url=base_url,
            endpoints={},
            retry=cast(
                RetryPolicy,
                retry_cfg(max_attempts=4, backoff=0.5, retry_on=[503]),
            ),
        )
        out = client.paginate_url(f'{base_url}/items', None)
        assert out == {'ok': True}

        # Should have slept twice (between the 3 attempts).
        assert len(capture_sleeps) == 2
        assert abs(capture_sleeps[0] - 0.1) < 1e-6
        assert abs(capture_sleeps[1] - 0.2) < 1e-6
        assert attempts['n'] == 3

    def test_retry_on_network_errors(
        self,
        base_url: str,
        capture_sleeps: list[float],
        retry_cfg: Callable[..., dict[str, Any]],
        jitter: Callable[[list[float]], list[float]],
        patch_request_once: Callable[[Callable[..., Any]], Callable[..., Any]],
        client_factory: Callable[..., EndpointClient],
    ) -> None:
        """
        Test that network errors are retried and sleep durations are captured.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        capture_sleeps : list[float]
            List to capture sleep durations.
        retry_cfg : Callable[..., dict[str, Any]]
            Factory for retry configuration.
        jitter : Callable[[list[float]], list[float]]
            Jitter function for sleep values.
        patch_request_once : Callable[[Callable[..., Any]], Callable[..., Any]]
            Helper used to patch the request helper.
        client_factory : Callable[..., EndpointClient]
            Factory fixture used to construct :class:`EndpointClient`.
        """
        # pylint: disable=unused-argument

        jitter([0.12, 0.18])
        attempts = {'n': 0}

        def _fake_request(
            self: EndpointClient,
            method: str,
            _url: str,
            *,
            session: Any,
            timeout: Any,
            **_kwargs: dict[str, Any],
        ) -> dict[str, Any]:
            assert method == 'GET'
            attempts['n'] += 1
            if attempts['n'] == 1:
                raise requests.Timeout('slow')
            if attempts['n'] == 2:
                raise requests.ConnectionError('reset')
            return {'ok': True}

        patch_request_once(_fake_request)

        client = client_factory(
            base_url=base_url,
            endpoints={},
            retry=cast(RetryPolicy, retry_cfg(max_attempts=4, backoff=0.5)),
            retry_network_errors=True,
        )
        out = client.paginate_url(f'{base_url}/items', None)
        assert out == {'ok': True}

        # Should have slept twice (after 2 failures).
        assert len(capture_sleeps) == 2
        assert abs(capture_sleeps[0] - 0.12) < 1e-6
        assert abs(capture_sleeps[1] - 0.18) < 1e-6
        assert attempts['n'] == 3


@pytest.mark.unit
class TestUrlComposition:
    """
    Unit test suite for URL composition in :class:`EndpointClient`.

    This suite covers:
    - Base path variants and endpoint joining
    - Query parameter merging and encoding
    - Path encoding and duplicate parameter handling
    - Query parameter ordering in composed URLs

    Notes
    -----
    - Uses pytest parameterization for variant coverage.
    - Ensures all composed URLs match expected output.
    """

    @pytest.mark.parametrize(
        ('base_url_suffix', 'base_path', 'endpoint', 'expected_suffix'),
        [
            ('', 'v2', 'items', '/v2/items'),
            ('', '/v2', '/items', '/v2/items'),
            ('/api', 'v1', '/items', '/api/v1/items'),
            # Note: trailing slashes on base_url/base_path not normalized by
            # client.
        ],
    )
    def test_base_path_variants(
        self,
        request_once_stub: dict[str, Any],
        base_url: str,
        base_url_suffix: str,
        base_path: str,
        endpoint: str,
        expected_suffix: str,
    ) -> None:
        """
        Test that base_path variants are composed correctly in URLs.

        Parameters
        ----------
        request_once_stub : dict[str, Any]
            Stub for capturing extracted URLs.
        base_url : str
            Base URL for the API.
        base_url_suffix : str
            Suffix to append to the base URL.
        base_path : str
            Base path for the API.
        endpoint : str
            Endpoint path.
        expected_suffix : str
            Expected suffix in the composed URL.
        """
        client = EndpointClient(
            base_url=f'{base_url}{base_url_suffix}',
            endpoints={'list': endpoint},
            base_path=base_path,
        )
        out = client.paginate('list', pagination=None)
        assert out == {'ok': True}
        assert request_once_stub['urls'] == [f'{base_url}{expected_suffix}']

    def test_query_merging_and_path_encoding(
        self,
        base_url: str,
        request_once_stub: dict[str, Any],
    ) -> None:
        """
        Test that query parameters are merged and path parameters are encoded.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        request_once_stub : dict[str, Any]
            Stub for capturing extracted URLs.
        """
        client = EndpointClient(
            base_url=f'{base_url}?existing=a&dup=1',
            endpoints={'item': '/users/{id}'},
        )

        out = client.paginate(
            'item',
            path_parameters={'id': 'A/B C'},
            query_parameters={'q': 'x y', 'dup': '2'},
            pagination=None,
        )
        assert out == {'ok': True}
        assert request_once_stub['urls'][0] == (
            f'{base_url}/users/A%2FB%20C?existing=a&dup=1&q=x+y&dup=2'
        )

    def test_query_merging_duplicate_base_params(
        self,
        base_url: str,
        request_once_stub: dict[str, Any],
    ) -> None:
        """
        Test that duplicate base query parameters are merged correctly.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        request_once_stub : dict[str, Any]
            Stub for capturing extracted URLs.
        """
        client = EndpointClient(
            base_url=f'{base_url}?dup=1&dup=2&z=9',
            endpoints={'e': '/ep'},
        )
        client.paginate(
            'e',
            query_parameters={'dup': '3', 'a': '1'},
            pagination=None,
        )
        assert request_once_stub['urls'][0] == (
            f'{base_url}/ep?dup=1&dup=2&z=9&dup=3&a=1'
        )

    def test_query_param_ordering(
        self,
        base_url: str,
        request_once_stub: dict[str, Any],
    ) -> None:
        """
        Test that query parameter ordering is preserved in composed URLs.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        request_once_stub : dict[str, Any]
            Stub for capturing extracted URLs.
        """
        client = EndpointClient(
            base_url=f'{base_url}?z=9&dup=1',
            endpoints={'e': '/ep'},
        )
        client.paginate(
            'e',
            query_parameters={'a': '1', 'dup': '2'},
            pagination=None,
        )
        assert request_once_stub['urls'][0] == (
            f'{base_url}/ep?z=9&dup=1&a=1&dup=2'
        )


@pytest.mark.property
class TestUrlCompositionProperty:
    """
    Unit tests for property-based URL composition using Hypothesis.

    This suite covers:
    - Path parameter encoding for arbitrary strings
    - Query parameter encoding for arbitrary dictionaries

    Notes
    -----
    - Uses Hypothesis strategies for robust property-based testing.
    - Ensures all encoded URLs match expected output.
    """

    @given(
        id_value=st.text(
            alphabet=st.characters(blacklist_categories=('Cs',)),
            min_size=1,
        ),
    )
    def test_path_parameter_encoding_property(
        self,
        base_url: str,
        id_value: str,
        extract_stub_factory: Callable[..., Any],
    ) -> None:
        """
        Test path parameter encoding in URLs.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        id_value : str
            Path parameter value to encode.
        extract_stub_factory : Callable[..., Any]
            Factory for extract stub (Hypothesis-safe).
        """
        with extract_stub_factory() as calls:  # type: ignore[call-arg]
            client = EndpointClient(
                base_url=base_url,
                endpoints={'item': '/users/{id}'},
            )
            client.paginate(
                'item',
                path_parameters={'id': id_value},
                pagination=None,
            )
            assert calls['urls'], 'no URL captured'
            url = calls['urls'].pop()
            parsed = urlparse.urlparse(url)
            expected_id = urlparse.quote(id_value, safe='')
            assert parsed.path.endswith('/users/' + expected_id)

    @given(
        params=st.dictionaries(
            keys=_ascii_no_amp_eq().filter(lambda s: len(s) > 0),
            values=_ascii_no_amp_eq(),
            min_size=1,
            max_size=5,
        ),
    )
    def test_query_encoding_property(
        self,
        base_url: str,
        params: dict[str, str],
        extract_stub_factory: Callable[..., Any],
    ) -> None:
        """
        Test query parameter encoding in URLs.

        Parameters
        ----------
        base_url : str
            Common base URL used across tests.
        params : dict[str, str]
            Query parameters to encode.
        extract_stub_factory : Callable[..., Any]
            Factory for extract stub (Hypothesis-safe).
        """
        with extract_stub_factory() as calls:  # type: ignore[call-arg]
            client = EndpointClient(
                base_url=base_url,
                endpoints={'e': '/ep'},
            )
            client.paginate('e', query_parameters=params, pagination=None)
            assert calls['urls'], 'no URL captured'
            url = calls['urls'].pop()
            parsed = urlparse.urlparse(url)
            round_params = dict(
                urlparse.parse_qsl(parsed.query, keep_blank_values=True),
            )
            assert round_params == params
