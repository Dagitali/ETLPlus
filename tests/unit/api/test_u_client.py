"""
``tests.unit.api.test_u_client`` module.

Unit tests for ``etlplus.api.client``.

Notes
-----
- Preserves path prefixes in composed URLs.
- Mocks network calls via patched extract helpers.
- Includes optional Hypothesis-based property tests when available.
"""
from __future__ import annotations

import types
import urllib.parse as urlparse
from typing import Any
from typing import Callable
from typing import cast

import pytest
import requests  # type: ignore[import]

import etlplus.api.client as cmod
from etlplus.api import CursorPaginationConfig
from etlplus.api import EndpointClient
from etlplus.api import errors as api_errors
from etlplus.api import PagePaginationConfig
from etlplus.api import RetryPolicy
from tests.unit.api.test_u_mocks import MockSession


# SECTION: HELPERS ========================================================== #


# Optional Hypothesis import with safe stubs when missing.
try:  # pragma: no try
    from hypothesis import given  # type: ignore[import-not-found]
    from hypothesis import strategies as st  # type: ignore[import-not-found]
    _HYP_AVAILABLE = True
except ImportError:  # pragma: no cover
    _HYP_AVAILABLE = False

    def given(*_a, **_k):  # type: ignore[unused-ignore]
        def _wrap(fn):
            return pytest.mark.skip(reason='needs hypothesis')(fn)
        return _wrap

    class _Strategy:  # minimal chainable strategy stub
        def filter(self, *_a, **_k):  # pragma: no cover
            return self

    class _DummyStrategies:
        def text(self, *_a, **_k):  # pragma: no cover
            return _Strategy()

        def characters(self, *_a, **_k):  # pragma: no cover
            return _Strategy()

        def dictionaries(self, *_a, **_k):  # pragma: no cover
            return _Strategy()

    st = _DummyStrategies()  # type: ignore[assignment]


def _ascii_no_amp_eq():  # type: ignore[missing-return-type]
    alpha = st.characters(min_codepoint=32, max_codepoint=126).filter(
        lambda ch: ch not in ['&', '='],
    )
    return st.text(alphabet=alpha, min_size=0, max_size=12)


def make_http_error(
    status: int,
) -> requests.HTTPError:
    err = requests.HTTPError(f"HTTP {status}")

    # Attach a response-like object that exposes status_code
    resp = requests.Response()
    resp.status_code = status
    err.response = resp  # type: ignore[attr-defined]

    return err


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestContextManager:
    """Unit test suite :class:`EndpointClient`."""

    def test_closes_factory_session(
        self,
        mock_session: MockSession,
    ) -> None:
        """
        Test that EndpointClient closes a session created by a factory.

        Parameters
        ----------
        mock_session : MockSession
            Mocked session object.
        """
        sess = mock_session
        client = EndpointClient(
            base_url='https://api.example.com',
            endpoints={},
            session_factory=lambda: sess,
        )
        with client:
            out = client.paginate_url(
                'https://api.example.com/items', None, None, None, None,
            )
            assert out == {'ok': True}
        assert sess.closed is True

    def test_creates_and_closes_default_session(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that EndpointClient creates and closes a default session.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        """

        # Substitute Session with MockSession to observe close()
        created: dict[str, MockSession] = {}

        def ctor() -> MockSession:
            s = MockSession()
            created['s'] = s
            return s

        # Patch extract to avoid network and capture params.
        monkeypatch.setattr(cmod.requests, 'Session', ctor)

        client = EndpointClient(
            base_url='https://api.example.com',
            endpoints={},
        )
        with client:
            out = client.paginate_url(
                'https://api.example.com/items', None, None, None, None,
            )
            assert out == {'ok': True}

        # After context exit, the created session should be closed.
        assert created['s'].closed is True

    def test_does_not_close_external_session(
        self,
        mock_session: MockSession,
    ) -> None:
        """
        Test that EndpointClient does not close an externally provided session.

        Parameters
        ----------
        mock_session : MockSession
            Mocked session object.
        """
        sess = mock_session
        client = EndpointClient(
            base_url='https://api.example.com',
            endpoints={},
            session=sess,
        )
        with client:
            out = client.paginate_url(
                'https://api.example.com/items', None, None, None, None,
            )
            assert out == {'ok': True}
        assert sess.closed is False


@pytest.mark.unit
class TestCursorPagination:
    """Unit test suite for :class:`EndpointClient`."""
    @pytest.mark.parametrize(
        'raw_page_size,expected_limit',
        [(-1, 1), ('not-a-number', EndpointClient.DEFAULT_PAGE_SIZE)],
    )
    def test_page_size_normalizes(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cursor_cfg: Callable[..., CursorPaginationConfig],
        raw_page_size: Any,
        expected_limit: int,
    ) -> None:
        """
        Test that page_size is normalized to a valid integer.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        cursor_cfg : Callable[..., CursorPaginationConfig]
            Factory for cursor pagination config.
        raw_page_size : Any
            Raw page size input.
        expected_limit : int
            Expected normalized limit.
        """
        calls: list[dict[str, Any]] = []

        def fake_extract(
            kind: str,
            _url: str,
            **kwargs: dict[str, Any],
        ):
            assert kind == 'api'
            calls.append(kwargs)

            # End after first page to keep test minimal.
            return {'items': [{'i': 1}], 'next': None}

        monkeypatch.setattr(cmod, '_extract', fake_extract)

        client = EndpointClient(base_url='https://example.test', endpoints={})
        cfg = cursor_cfg(
            cursor_param='cursor',
            cursor_path='next',
            page_size=raw_page_size,
            records_path='items',
        )
        out = client.paginate_url(
            'https://example.test/x',
            None,
            None,
            None,
            cfg,
        )
        assert isinstance(out, list)

        # mypy treats list element as Any due to external library response
        # type.
        items = [cast(dict, r)['i'] for r in out]  # type: ignore[index]
        assert items == [1]
        params = calls[0].get('params', {})
        assert params.get('limit') == expected_limit

    def test_adds_limit_and_advances_cursor(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cursor_cfg: Callable[..., CursorPaginationConfig],
    ) -> None:
        """
        Test that limit is added and cursor advances correctly.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        cursor_cfg : Callable[..., CursorPaginationConfig]
            Factory for cursor pagination config.
        """
        calls: list[dict[str, Any]] = []

        def fake_extract(
            kind: str,
            _url: str,
            **kwargs: dict[str, Any],
        ):
            assert kind == 'api'
            calls.append(kwargs)
            params = kwargs.get('params') or {}
            if 'cursor' not in params:
                return {'items': [{'i': 1}], 'next': 'abc'}
            return {'items': [{'i': 2}], 'next': None}

        monkeypatch.setattr(cmod, '_extract', fake_extract)
        client = EndpointClient(base_url='https://example.test', endpoints={})
        cfg = cursor_cfg(
            cursor_param='cursor',
            cursor_path='next',
            page_size=10,
            records_path='items',
        )
        data = client.paginate_url(
            'https://example.test/x',
            None,
            None,
            None,
            cfg,
        )
        assert isinstance(data, list)
        assert len(calls) >= 2
        values = [cast(dict, r)['i'] for r in data]  # type: ignore[index]
        assert values == [1, 2]
        first = calls[0]
        second = calls[1]
        assert first.get('params', {}).get('limit') == 10
        assert 'cursor' not in (first.get('params') or {})
        assert second.get('params', {}).get('cursor') == 'abc'
        assert second.get('params', {}).get('limit') == 10

    def test_error_includes_page_number(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cursor_cfg: Callable[..., CursorPaginationConfig],
    ) -> None:
        """
        Test that PaginationError includes the page number on failure.

        When a cursor-paginated request fails, PaginationError includes page.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        cursor_cfg : Callable[..., CursorPaginationConfig]
            Factory for cursor pagination config.
        """
        client = EndpointClient(
            base_url='https://api.example.com/v1',
            endpoints={'list': '/items'},
        )

        # First page succeeds with next cursor; second raises 500.
        calls = {'n': 0}

        def extractor(
            _stype: str,
            _url: str, **kwargs: dict[str, Any],
        ):
            calls['n'] += 1
            if calls['n'] == 1:
                return {
                    'items': [{'i': 1}],
                    'meta': {'next': 'xyz'},
                }
            raise make_http_error(500)

        monkeypatch.setattr(cmod, '_extract', extractor)

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

    def test_retry_backoff_sleeps(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cursor_cfg: Callable[..., CursorPaginationConfig],
        capture_sleeps: list[float],
        jitter: Callable[[list[float]], list[float]],
    ) -> None:
        """
        Test that cursor pagination applies retry backoff sleep on failure.

        Cursor pagination applies retry backoff sleep on failure

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        cursor_cfg : Callable[..., CursorPaginationConfig]
            Factory for cursor pagination config.
        capture_sleeps : list[float]
            List to capture sleep durations.
        jitter : Callable[[list[float]], list[float]]
            Jitter function for sleep values.
        """
        jitter([0.05])

        attempts = {'n': 0}

        def fake_extract(kind: str, _url: str, **_k: Any):
            assert kind == 'api'
            attempts['n'] += 1
            if attempts['n'] == 1:
                err = requests.HTTPError('boom')
                err.response = types.SimpleNamespace(status_code=503)
                raise err
            return {'items': [{'i': 1}], 'next': None}

        monkeypatch.setattr(cmod, '_extract', fake_extract)
        client = EndpointClient(
            base_url='https://example.test',
            endpoints={},
            retry={'max_attempts': 2, 'backoff': 0.5, 'retry_on': [503]},
        )
        cfg = cursor_cfg(
            cursor_param='cursor',
            cursor_path='next',
            page_size=2,
            records_path='items',
        )

        out = client.paginate_url(
            'https://example.test/x',
            None,
            None,
            None,
            cfg,
        )
        assert out == [{'i': 1}]

        # One sleep from the single retry attempt.
        assert len(capture_sleeps) == 1
        assert abs(capture_sleeps[0] - 0.05) < 1e-6
        assert attempts['n'] == 2


@pytest.mark.unit
class TestErrors:
    """
    Unit test suite for :class:`ApiAuthError`.
    """

    def test_auth_error_wrapping_on_single_attempt(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that ApiAuthError is raised and wrapped on a single attempt.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.

        """
        client = EndpointClient(
            base_url='https://api.example.com/v1',
            endpoints={'x': '/x'},
        )

        def boom(
            _stype: str,
            url: str,
            **kwargs: dict[str, Any],  # noqa: ARG001
        ):
            raise make_http_error(401)

        monkeypatch.setattr(cmod, '_extract', boom)
        with pytest.raises(api_errors.ApiAuthError) as ei:
            client.paginate_url(
                'https://api.example.com/v1/x', None, None, None, None,
            )
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
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that offset pagination behaves as expected.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        """
        calls: list[dict[str, Any]] = []

        def fake_extract(kind: str, _url: str, **kwargs: dict[str, Any]):
            assert kind == 'api'
            calls.append(kwargs)
            params = kwargs.get('params') or {}
            off = int(params.get('offset', 0))
            limit = int(params.get('limit', 2))
            # return exactly `limit` items until offset reaches 4
            if off >= 4:
                return []
            return [{'i': i} for i in range(off, off + limit)]

        monkeypatch.setattr(cmod, '_extract', fake_extract)

        client = EndpointClient(base_url='https://example.test', endpoints={})
        cfg = cast(
            PagePaginationConfig,
            {
                'type': 'offset',
                'page_param': 'offset',
                'size_param': 'limit',
                'start_page': 0,
                'page_size': 2,
                'max_records': 3,
            },
        )

        data = client.paginate_url(
            'https://example.test/api',
            None,
            None,
            None,
            cfg,
        )

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
        monkeypatch: pytest.MonkeyPatch,
        page_cfg: Callable[..., PagePaginationConfig],
    ) -> None:
        """
        Test that pagination stops on a short final batch.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        page_cfg : Callable[..., PagePaginationConfig]
            Factory for page pagination config.
        """
        def fake_extract(
            kind: str,
            _url: str,
            **kwargs: dict[str, Any],
        ):
            assert kind == 'api'
            page = int((kwargs.get('params') or {}).get('page', 1))
            if page == 1:
                return [{'id': 1}, {'id': 2}]
            if page == 2:
                return [{'id': 3}]
            return []

        monkeypatch.setattr(cmod, '_extract', fake_extract)
        client = EndpointClient(base_url='https://example.test', endpoints={})
        cfg = page_cfg(
            page_param='page',
            size_param='per_page',
            start_page=1,
            page_size=2,
        )
        data = client.paginate_url(
            'https://example.test/api',
            None,
            None,
            5,
            cfg,
        )
        assert isinstance(data, list)
        ids = [cast(dict, r)['id'] for r in data]  # type: ignore[index]
        assert ids == [1, 2, 3]

    def test_max_records_cap(
        self,
        monkeypatch: pytest.MonkeyPatch,
        page_cfg: Callable[..., PagePaginationConfig],
    ) -> None:
        """
        Test that max_records parameter truncates results as expected.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        page_cfg : Callable[..., PagePaginationConfig]
            Factory for page pagination config.
        """
        def fake_extract(
            kind: str,
            _url: str,
            **kwargs: dict[str, Any],
        ):
            assert kind == 'api'
            page = int((kwargs.get('params') or {}).get('page', 1))

            # Each page returns 3 records to force truncation.
            return [{'p': page, 'i': i} for i in range(3)]

        monkeypatch.setattr(cmod, '_extract', fake_extract)

        client = EndpointClient(base_url='https://example.test', endpoints={})
        cfg = page_cfg(
            page_param='page',
            size_param='per_page',
            start_page=1,
            page_size=3,
            max_records=5,  # Should truncate 2nd page (total would be 6).
        )
        data = client.paginate_url(
            'https://example.test/x',
            None,
            None,
            None,
            cfg,
        )
        assert len(data) == 5
        assert all('p' in r for r in data)

    def test_page_size_normalization(
        self,
        monkeypatch: pytest.MonkeyPatch,
        page_cfg: Callable[..., PagePaginationConfig],
    ) -> None:
        """
        Test that page_size is normalized to 1 if set to 0.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        page_cfg : Callable[..., PagePaginationConfig]
            Factory for page pagination config.
        """
        def fake_extract(kind: str, _url: str, **kw: Any):
            assert kind == 'api'
            params = kw.get('params') or {}

            page = int(params.get('page', 1))

            # Return single record; page_size gets normalized to 1.
            return [{'id': page}]

        monkeypatch.setattr(cmod, '_extract', fake_extract)

        client = EndpointClient(base_url='https://example.test', endpoints={})
        cfg = page_cfg(
            page_param='page',
            size_param='per_page',
            start_page=1,
            page_size=0,
            max_pages=3,
        )
        data = client.paginate_url(
            'https://example.test/x',
            None,
            None,
            None,
            cfg,
        )
        assert isinstance(data, list)
        ids = [cast(dict, r)['id'] for r in data]  # type: ignore[index]
        assert ids == [1, 2, 3]

    def test_error_includes_page_number(
        self,
        monkeypatch: pytest.MonkeyPatch,
        page_cfg: Callable[..., PagePaginationConfig],
    ) -> None:
        """
        Test that PaginationError includes the page number on failure.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        page_cfg : Callable[..., PagePaginationConfig]
            Factory for page pagination config.
        """
        client = EndpointClient(
            base_url='https://api.example.com/v1',
            endpoints={'list': '/items'},
        )
        page_size = 2

        def extractor(
            _stype: str,
            _url: str,
            **kwargs: dict[str, Any],
        ):
            params = kwargs.get('params') or {}
            page = int(params.get('page', 1))
            size = int(params.get('per_page', page_size))
            if page == 4:
                raise make_http_error(500)
            return {'items': [{'i': i} for i in range(size)]}

        # Return exactly `size` records to force continue until failure.
        monkeypatch.setattr(cmod, '_extract', extractor)
        cfg = page_cfg(
            page_param='page',
            size_param='per_page',
            start_page=3,
            page_size=page_size,
            records_path='items',
        )

        with pytest.raises(api_errors.PaginationError) as ei:
            client.paginate('list', pagination=cfg)
        # assert ei.value.page == 4 and ei.value.status == 500
        assert ei.value.page == 2 and ei.value.status == 500

    def test_unknown_type_returns_raw(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that unknown pagination type returns raw output.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        """
        monkeypatch.setattr(
            cmod,
            '_extract',
            lambda k, _u, **kwargs: {'foo': 'bar'} if k == 'api' else None,
        )
        client = EndpointClient(base_url='https://example.test', endpoints={})

        out = client.paginate_url(
            'https://example.test/x',
            None,
            None,
            None,
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
        monkeypatch: pytest.MonkeyPatch,
        capture_sleeps: list[float],
    ) -> None:
        """
        Test that explicit sleep_seconds overrides rate_limit config.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        capture_sleeps : list[float]
            List to capture sleep durations.
        """
        # Simulate apply_sleep capturing values already via capture_sleeps.
        client = EndpointClient(
            base_url='https://api.example.com/v1',
            endpoints={'list': '/items'},
            rate_limit={'max_per_sec': 2},  # would imply 0.5s if used
        )

        # Patch _extract to return enough pages for two sleeps in page mode.
        calls = {'n': 0}

        def fake_extract(kind: str, url: str, **kw: Any):  # noqa: D401
            calls['n'] += 1
            # Return full page until third call which ends pagination.
            if calls['n'] < 3:
                return [{'i': calls['n']}, {'i': calls['n'] + 10}]
            return []

        monkeypatch.setattr(cmod, '_extract', fake_extract)

        list(
            client.paginate_iter(
                'list',
                pagination=cast(
                    PagePaginationConfig,
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

    Tests retry behavior for request errors, jitter backoff, and network error
    handling, ensuring correct retry policy application and error propagation.
    """

    def test_request_error_after_retries_exhausted(
        self,
        monkeypatch: pytest.MonkeyPatch,
        retry_cfg: Callable[..., dict[str, Any]],
    ) -> None:
        """
        Test that ApiRequestError is raised after retries are exhausted.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        retry_cfg : Callable[..., dict[str, Any]]
            Factory for retry configuration.

        """
        client = EndpointClient(
            base_url='https://api.example.com/v1',
            endpoints={'x': '/x'},
            retry=cast(
                RetryPolicy,
                retry_cfg(max_attempts=2, backoff=0.0, retry_on=[503]),
            ),
        )
        attempts = {'n': 0}

        def boom(
            _stype: str,
            _url: str,
            **kwargs: dict[str, Any],
        ):  # noqa: ARG001
            attempts['n'] += 1
            raise make_http_error(503)

        monkeypatch.setattr(cmod, '_extract', boom)

        with pytest.raises(api_errors.ApiRequestError) as ei:
            client.paginate_url(
                'https://api.example.com/v1/x', None, None, None, None,
            )
        err = ei.value
        assert isinstance(err, api_errors.ApiRequestError)
        assert err.status == 503
        assert err.attempts == 2  # Exhausted
        assert err.retried is True

    def test_full_jitter_backoff(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_sleeps: list[float],
        retry_cfg: Callable[..., dict[str, Any]],
        jitter: Callable[[list[float]], list[float]],
    ) -> None:
        """
        Test that full jitter backoff is applied on retries.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        capture_sleeps : list[float]
            List to capture sleep durations.
        retry_cfg : Callable[..., dict[str, Any]]
            Factory for retry configuration.
        jitter : Callable[[list[float]], list[float]]
            Jitter function for sleep values.
        """
        jitter([0.1, 0.2])

        # Patch _extract in client module to fail with 503 twice, then succeed.
        attempts = {'n': 0}

        def _fake_extract(
            _stype: str,
            _url: str,
            **kwargs: dict[str, Any],
        ):
            attempts['n'] += 1
            if attempts['n'] < 3:
                err = requests.HTTPError('boom')
                err.response = types.SimpleNamespace(status_code=503)
                raise err
            return {'ok': True}

        monkeypatch.setattr(cmod, '_extract', _fake_extract)

        client = EndpointClient(
            base_url='https://api.example.com',
            endpoints={},
            retry=cast(
                RetryPolicy,
                retry_cfg(max_attempts=4, backoff=0.5, retry_on=[503]),
            ),
        )
        out = client.paginate_url(
            'https://api.example.com/items', None, None, None, None,
        )
        assert out == {'ok': True}

        # Should have slept twice (between the 3 attempts).
        assert len(capture_sleeps) == 2
        assert abs(capture_sleeps[0] - 0.1) < 1e-6
        assert abs(capture_sleeps[1] - 0.2) < 1e-6
        assert attempts['n'] == 3

    def test_retry_on_network_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_sleeps: list[float],
        retry_cfg: Callable[..., dict[str, Any]],
        jitter: Callable[[list[float]], list[float]],
    ) -> None:
        """
        Test that network errors are retried and sleep durations are captured.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture.
        capture_sleeps : list[float]
            List to capture sleep durations.
        retry_cfg : Callable[..., dict[str, Any]]
            Factory for retry configuration.
        jitter : Callable[[list[float]], list[float]]
            Jitter function for sleep values.
        """
        jitter([0.12, 0.18])
        attempts = {'n': 0}

        def _fake_extract(
            _stype: str,
            _url: str,
            **kwargs: dict[str, Any],
        ):
            attempts['n'] += 1
            if attempts['n'] == 1:
                raise requests.Timeout('slow')
            if attempts['n'] == 2:
                raise requests.ConnectionError('reset')
            return {'ok': True}

        monkeypatch.setattr(cmod, '_extract', _fake_extract)

        client = EndpointClient(
            base_url='https://api.example.com',
            endpoints={},
            retry=cast(RetryPolicy, retry_cfg(max_attempts=4, backoff=0.5)),
            retry_network_errors=True,
        )
        out = client.paginate_url(
            'https://api.example.com/items', None, None, None, None,
        )
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

    Tests base path variants, query parameter merging, path encoding, and
    query parameter ordering in composed URLs.
    """
    @pytest.mark.parametrize(
        'base_url,base_path,endpoint,expected_url',
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
            # Note: trailing slashes on base_url/base_path not normalized by
            # client.

        ],
    )
    def test_base_path_variants(
        self,
        extract_stub: dict[str, Any],
        base_url: str,
        base_path: str,
        endpoint: str,
        expected_url: str,
    ) -> None:
        """
        Test that base_path variants are composed correctly in URLs.

        Parameters
        ----------
        extract_stub : dict[str, Any]
            Stub for capturing extracted URLs.
        base_url : str
            Base URL for the API.
        base_path : str
            Base path for the API.
        endpoint : str
            Endpoint path.
        expected_url : str
            Expected composed URL.
        """
        client = EndpointClient(
            base_url=base_url,
            endpoints={'list': endpoint},
            base_path=base_path,
        )
        out = client.paginate('list', pagination=None)
        assert out == {'ok': True}
        assert extract_stub['urls'] == [expected_url]

    def test_query_merging_and_path_encoding(
        self,
        extract_stub: dict[str, Any],
    ) -> None:
        """
        Test that query parameters are merged and path parameters are encoded.

        Parameters
        ----------
        extract_stub : dict[str, Any]
            Stub for capturing extracted URLs.
        """
        client = EndpointClient(
            base_url='https://api.example.com/v1?existing=a&dup=1',
            endpoints={'item': '/users/{id}'},
        )

        out = client.paginate(
            'item',
            path_parameters={'id': 'A/B C'},
            query_parameters={'q': 'x y', 'dup': '2'},
            pagination=None,
        )
        assert out == {'ok': True}
        assert extract_stub['urls'][0] == (
            'https://api.example.com/v1/users/A%2FB%20C?'
            'existing=a&dup=1&q=x+y&dup=2'
        )

    def test_query_merging_duplicate_base_params(
        self,
        extract_stub: dict[str, Any],
    ) -> None:
        """
        Test that duplicate base query parameters are merged correctly.

        Parameters
        ----------
        extract_stub : dict[str, Any]
            Stub for capturing extracted URLs.
        """
        client = EndpointClient(
            base_url='https://api.example.com/v1?dup=1&dup=2&z=9',
            endpoints={'e': '/ep'},
        )
        client.paginate(
            'e',
            query_parameters={'dup': '3', 'a': '1'},
            pagination=None,
        )
        assert extract_stub['urls'][0] == (
            'https://api.example.com/v1/ep?dup=1&dup=2&z=9&dup=3&a=1'
        )

    def test_query_param_ordering(
        self,
        extract_stub: dict[str, Any],
    ) -> None:
        """
        Test that query parameter ordering is preserved in composed URLs.

        Parameters
        ----------
        extract_stub : dict[str, Any]
            Stub for capturing extracted URLs.
        """
        client = EndpointClient(
            base_url='https://api.example.com/v1?z=9&dup=1',
            endpoints={'e': '/ep'},
        )
        client.paginate(
            'e', query_parameters={'a': '1', 'dup': '2'}, pagination=None,
        )
        assert extract_stub['urls'][0] == (
            'https://api.example.com/v1/ep?z=9&dup=1&a=1&dup=2'
        )


@pytest.mark.property
class TestUrlCompositionProperty:
    """Property-based URL composition tests (Hypothesis)."""

    @given(
        id_value=st.text(
            alphabet=st.characters(blacklist_categories=('Cs',)),
            min_size=1,
        ),
    )
    def test_path_parameter_encoding_property(
        self,
        id_value: str,
        extract_stub_factory: Callable[..., Any],
    ) -> None:
        """
        Property-based test for path parameter encoding in URLs.

        Parameters
        ----------
        id_value : str
            Path parameter value to encode.
        extract_stub_factory : Callable[..., Any]
            Factory for extract stub (Hypothesis-safe).
        """
        with extract_stub_factory() as calls:  # type: ignore[call-arg]
            client = EndpointClient(
                base_url='https://api.example.com/v1',
                endpoints={'item': '/users/{id}'},
            )
            client.paginate(
                'item', path_parameters={'id': id_value}, pagination=None,
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
        params: dict[str, str],
        extract_stub_factory: Callable[..., Any],
    ) -> None:
        """
        Property-based test for query parameter encoding in URLs.

        Parameters
        ----------
        params : dict[str, str]
            Query parameters to encode.
        extract_stub_factory : Callable[..., Any]
            Factory for extract stub (Hypothesis-safe).
        """
        with extract_stub_factory() as calls:  # type: ignore[call-arg]
            client = EndpointClient(
                base_url='https://api.example.com/v1',
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
