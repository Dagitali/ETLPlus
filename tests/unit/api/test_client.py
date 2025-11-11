"""
tests.unit.api.test_client unit tests module.

Modernized tests for the EndpointClient.

Focus: URL composition, pagination, retries, and session handling.

Verify that when a client is constructed with a base URL that includes a path
prefix (e.g., an API's effective base URL with base_path), calling
``paginate(endpoint_key, ...)`` builds URLs that include that path.

This is a pure URL-building test; network calls are mocked by patching the
module-level ``_extract`` function used by ``EndpointClient``.
"""
from __future__ import annotations

import types
import urllib.parse as urlparse
from typing import Any
from typing import cast

import pytest
import requests  # type: ignore[import]

import etlplus.api.client as cmod
from etlplus.api import EndpointClient
from etlplus.api import errors as api_errors
from etlplus.api import PagePaginationConfig
from tests.unit.api.test_mocks import MockSession


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


class TestContextManager:
    def test_closes_factory_session(
        self,
        monkeypatch: pytest.MonkeyPatch,
        extract_stub: dict[str, Any],  # noqa: ARG001 - _extract patched
        mock_session: MockSession,
    ) -> None:
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
        extract_stub: dict[str, Any],  # noqa: ARG001 - _extract patched
    ) -> None:
        # Patch extract to avoid network and capture params.

        # Substitute Session with MockSession to observe close()
        created: dict[str, MockSession] = {}

        def ctor() -> MockSession:
            s = MockSession()
            created['s'] = s
            return s

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
        monkeypatch: pytest.MonkeyPatch,
        extract_stub: dict[str, Any],  # noqa: ARG001 - _extract patched
        mock_session: MockSession,
    ) -> None:
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


class TestCursorPagination:
    @pytest.mark.parametrize(
        'raw_page_size,expected_limit',
        [(-1, 1), ('not-a-number', EndpointClient.DEFAULT_PAGE_SIZE)],
    )
    def test_page_size_normalizes(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cursor_cfg,  # fixture from conftest
        raw_page_size: Any,
        expected_limit: int,
    ) -> None:
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
        cursor_cfg,  # fixture from conftest
    ) -> None:
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
        cursor_cfg,  # fixture from conftest
    ) -> None:
        """
        When a cursor-paginated request fails, PaginationError includes page.
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
        cursor_cfg,  # fixture from conftest
        capture_sleeps: list[float],
        jitter,
    ) -> None:
        """Cursor pagination applies retry backoff sleep on failure."""
        jitter([0.05])

        attempts = {'n': 0}

        def fake_extract(kind: str, _url: str, **_k: Any):
            assert kind == 'api'
            attempts['n'] += 1
            if attempts['n'] == 1:
                err = requests.HTTPError('boom')
                err.response = types.SimpleNamespace(
                    status_code=503,
                )
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
        assert capture_sleeps == [pytest.approx(0.05)]
        assert attempts['n'] == 2


class TestErrors:
    def test_auth_error_wrapping_on_single_attempt(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
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


class TestOffsetPagination:
    def test_offset_pagination_behaves_like_offset(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
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

        # Expected behavior: collects up to max_records using offset stepping
        assert [r['i'] for r in cast(list[dict[str, int]], data)] == [0, 1, 2]


class TestPagePagination:
    def test_stops_on_short_final_batch(
        self,
        monkeypatch: pytest.MonkeyPatch,
        page_cfg,  # fixture from conftest
    ) -> None:
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
        page_cfg,  # fixture from conftest
    ) -> None:
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
        page_cfg,  # fixture from conftest
    ) -> None:
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
        page_cfg,  # fixture from conftest
    ) -> None:
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
        assert ei.value.page == 4 and ei.value.status == 500

    def test_unknown_type_returns_raw(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
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


class TestRateLimitPrecedence:
    def test_overrides_sleep_seconds_wins(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_sleeps: list[float],
    ) -> None:
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
                pagination={
                    'type': 'page',
                    'page_size': 2,
                    'start_page': 1,
                },
                sleep_seconds=0.05,  # explicit override should win
            ),
        )

        # We expect exactly two sleeps (between three page fetches) and both
        # should reflect explicit override (0.05) not derived 0.5.
        assert capture_sleeps == [pytest.approx(0.05), pytest.approx(0.05)]


class TestRetryLogic:
    def test_request_error_after_retries_exhausted(
        self,
        monkeypatch: pytest.MonkeyPatch,
        retry_cfg,
    ) -> None:
        client = EndpointClient(
            base_url='https://api.example.com/v1',
            endpoints={'x': '/x'},
            retry=retry_cfg(max_attempts=2, backoff=0.0, retry_on=[503]),
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
        retry_cfg,
        jitter,
    ) -> None:
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
                err.response = types.SimpleNamespace(
                    status_code=503,
                )
                raise err
            return {'ok': True}

        monkeypatch.setattr(cmod, '_extract', _fake_extract)

        client = EndpointClient(
            base_url='https://api.example.com',
            endpoints={},
            retry=retry_cfg(max_attempts=4, backoff=0.5, retry_on=[503]),
        )
        out = client.paginate_url(
            'https://api.example.com/items', None, None, None, None,
        )
        assert out == {'ok': True}

        # Should have slept twice (between the 3 attempts).
        assert capture_sleeps == [0.1, 0.2]
        assert attempts['n'] == 3

    def test_retry_on_network_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_sleeps: list[float],
        retry_cfg,
        jitter,
    ) -> None:
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
            retry=retry_cfg(max_attempts=4, backoff=0.5),
            retry_network_errors=True,
        )
        out = client.paginate_url(
            'https://api.example.com/items', None, None, None, None,
        )
        assert out == {'ok': True}

        # Should have slept twice (after 2 failures).
        assert capture_sleeps == [0.12, 0.18]
        assert attempts['n'] == 3


class TestUrlComposition:
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
            # Note: trailing slashes on base_url/base_path not normalized by
            # client.

        ],
    )
    def test_base_path_variants(
        self,
        monkeypatch: pytest.MonkeyPatch,
        extract_stub: dict[str, Any],  # noqa: ARG001 - _extract patched
        base_url: str,
        base_path: str,
        endpoint: str,
        expected: str,
    ) -> None:
        client = EndpointClient(
            base_url=base_url,
            endpoints={'list': endpoint},
            base_path=base_path,
        )
        out = client.paginate('list', pagination=None)
        assert out == {'ok': True}
        assert extract_stub['urls'] == [expected]

    def test_query_merging_and_path_encoding(
        self,
        monkeypatch: pytest.MonkeyPatch,
        extract_stub: dict[str, Any],  # noqa: ARG001 - _extract patched
    ) -> None:

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
        monkeypatch: pytest.MonkeyPatch,
        extract_stub: dict[str, Any],  # noqa: ARG001 - _extract patched
    ) -> None:

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
        self, monkeypatch: pytest.MonkeyPatch,
        extract_stub: dict[str, Any],  # noqa: ARG001 - _extract patched
    ) -> None:

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
        extract_stub_factory,  # session-scoped factory, Hypothesis-safe
    ) -> None:
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
        extract_stub_factory,  # session-scoped factory, Hypothesis-safe
    ) -> None:
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
