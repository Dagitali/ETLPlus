"""
:mod:`tests.unit.api.test_u_api_paginator` module.

Unit tests for :class:`etlplus.api.pagination.Paginator`.

Notes
-----
- Exercises pagination defaults, cursor helpers, and record extraction.
- Ensures thin :class:`EndpointClient` wrappers delegate to
    ``paginate_url_iter``.
- Verifies the optional :class:`RateLimiter` integration for pacing between
    page fetches.

Examples
--------
>>> pytest tests/unit/api/test_u_paginator.py
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from typing import cast

import pytest

from etlplus.api import EndpointClient
from etlplus.api import PaginationConfig
from etlplus.api import RateLimiter
from etlplus.api import RequestOptions
from etlplus.api.errors import ApiRequestError
from etlplus.api.errors import PaginationError
from etlplus.api.pagination import PagePaginationConfigDict
from etlplus.api.pagination import PaginationInput
from etlplus.api.pagination import PaginationType
from etlplus.api.pagination import Paginator
from etlplus.api.rate_limiting import RateLimitConfigDict

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def _dummy_fetch(
    url: str,
    request: RequestOptions,
    page: int | None,
) -> dict[str, Any]:
    """Simple fetch stub that echoes input for Paginator construction."""
    return {'url': url, 'params': request.params or {}, 'page': page}


class RecordingClient(EndpointClient):
    """
    EndpointClient subclass that records paginate_url_iter calls.

    Used to verify that ``paginate`` and ``paginate_iter`` are thin shims
    over ``paginate_url_iter``.
    """

    _paginate_calls: list[dict[str, Any]] = []

    @property
    def paginate_calls(self) -> list[dict[str, Any]]:
        """Access recorded :meth:`paginate_url_iter` calls."""
        return type(self)._paginate_calls

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        type(self)._paginate_calls.clear()

    def paginate_url_iter(
        self,
        url: str,
        pagination: PaginationInput = None,
        *,
        request: RequestOptions | None = None,
        sleep_seconds: float = 0.0,
        rate_limit_overrides: RateLimitConfigDict | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Record arguments and yield a single marker record."""
        type(self)._paginate_calls.append(
            {
                'url': url,
                'pagination': pagination,
                'request': request,
                'sleep_seconds': sleep_seconds,
                'rate_limit_overrides': rate_limit_overrides,
            },
        )
        yield {'marker': 'ok'}


class FakePageClient(EndpointClient):
    """
    EndpointClient subclass that simulates paginated results.

    Used to test :class:`Paginator` integration without real HTTP calls.
    """

    def paginate_url_iter(
        self,
        url: str,
        pagination: PaginationInput = None,
        *,
        request: RequestOptions | None = None,
        sleep_seconds: float = 0.0,
        rate_limit_overrides: RateLimitConfigDict | None = None,
    ) -> Iterator[dict[str, Any]]:
        # Ignore all arguments; just simulate three records from two pages.
        _ = request  # keep signature compatibility while avoiding unused var
        yield {'id': 1}
        yield {'id': 2}
        yield {'id': 3}


# SECTION: TESTS ============================================================ #


class TestPaginator:
    """Unit tests for :class:`Paginator`."""

    def test_coalesce_records_uses_fallback_path(self) -> None:
        """
        Test that :meth:`coalesce_records` falls back when primary path is
        empty.
        """
        payload = {
            'data': {
                'primary': [],
                'backup': [{'id': 99}],
            },
        }

        records = Paginator.coalesce_records(
            payload,
            'data.primary',
            'data.backup',
        )

        assert records == [{'id': 99}]

    def test_defaults_when_missing_keys(self) -> None:
        """
        Test that default parameter names and limits are preserved.

        Notes
        -----
        - When optional pagination configuration keys are omitted, the
            paginator should fall back to its class-level defaults.
        """
        cfg: PagePaginationConfigDict = {'type': PaginationType.PAGE}

        paginator = Paginator.from_config(cfg, fetch=_dummy_fetch)

        assert (
            paginator.page_param == Paginator.PAGE_PARAMS[PaginationType.PAGE]
        )
        assert (
            paginator.size_param == Paginator.SIZE_PARAMS[PaginationType.PAGE]
        )
        assert paginator.limit_param == Paginator.LIMIT_PARAM
        assert paginator.cursor_param == Paginator.CURSOR_PARAM
        assert paginator.records_path is None
        assert paginator.cursor_path is None
        assert paginator.max_pages is None
        assert paginator.max_records is None
        assert paginator.start_cursor is None

    def test_page_integration(self) -> None:
        """
        Test pagination over a multi-record iterator.

        Uses a lightweight EndpointClient subclass that overrides
        ``paginate_url_iter`` to simulate multiple pages of results and
        verifies that ``paginate`` flattens them into a single record stream.
        """

        client = FakePageClient(
            base_url='https://example.test/api',
            endpoints={'items': '/items'},
        )

        pg: PagePaginationConfigDict = {
            'type': PaginationType.PAGE,
            'page_param': 'page',
            'size_param': 'per_page',
            'page_size': 2,
        }

        records = cast(
            list[dict[str, Any]],
            list(client.paginate('items', pagination=pg)),
        )

        expected = [1, 2, 3]
        for i, r in enumerate(records):
            assert r.get('id') in expected
            assert r.get('id') == expected[i]

    @pytest.mark.parametrize(
        ('actual', 'expected_page_size'),
        [
            (None, Paginator.PAGE_SIZE),
            (-1, 1),
            (0, 1),
            (1, 1),
            (50, 50),
        ],
    )
    def test_page_size_normalization(
        self,
        actual: int | None,
        expected_page_size: int,
    ) -> None:
        """
        Test that ``page_size`` values are coerced to a positive integer.
        """
        cfg: PagePaginationConfigDict = {'type': PaginationType.PAGE}
        if actual is not None:
            cfg['page_size'] = actual

        paginator = Paginator.from_config(cfg, fetch=_dummy_fetch)
        assert paginator.page_size == expected_page_size

    def test_paginate_accepts_request_options(self) -> None:
        """
        Test that :meth:`Paginator.paginate` accepts :class:`RequestOptions`
        overrides for params.
        """
        seen: list[RequestOptions] = []

        def fetch(
            _url: str,
            request: RequestOptions,
            page: int | None,
        ) -> dict[str, Any]:
            seen.append(request)
            return {'items': []}

        paginator = Paginator.from_config(
            {
                'type': PaginationType.PAGE,
                'records_path': 'items',
            },
            fetch=fetch,
        )

        seed = RequestOptions(headers={'X': 'seed'}, params={'initial': 1})
        override = seed.evolve(params={'page': 3})
        list(
            paginator.paginate(
                'https://example.test/items',
                request=override,
            ),
        )

        assert seen
        first = seen[0]
        assert first.params is not None
        assert first.params.get('page') == 3
        assert first.params.get(paginator.size_param) == paginator.page_size
        assert first.headers == {'X': 'seed'}

    def test_paginate_and_paginate_iter_are_thin_shims(self) -> None:
        """
        Test that paginate and paginate_iter delegate to paginate_url_iter.
        """
        client = RecordingClient(
            base_url='https://example.test/api',
            endpoints={'items': '/items'},
        )

        pg: PagePaginationConfigDict = {'type': PaginationType.PAGE}

        # Both helpers should route through paginate_url_iter.
        list(client.paginate('items', pagination=pg))
        list(client.paginate_iter('items', pagination=pg))

        # Both calls should have gone through paginate_url_iter exactly once
        # each.
        assert len(client._paginate_calls) == 2

        calls: list[dict[str, Any]] = client._paginate_calls

        urls = [call['url'] for call in calls]
        assert urls == [
            'https://example.test/api/items',
            'https://example.test/api/items',
        ]

        paginations = [call['pagination'] for call in calls]
        assert paginations == [pg, pg]

    def test_rate_limiter_enforces_between_pages(
        self,
    ) -> None:
        """Test that the configured rate limiter enforces pacing."""

        payloads = [
            {'items': [{'id': 1}]},
            {'items': [{'id': 2}]},
            {'items': []},
        ]

        def fetch(
            _url: str,
            _request: RequestOptions,
            _page: int | None,
        ) -> dict[str, Any]:
            return cast(dict[str, Any], payloads.pop(0))

        limiter_calls: list[int] = []

        class DummyLimiter(RateLimiter):
            """Dummy RateLimiter that records enforce calls."""

            def __init__(self) -> None:  # pragma: no cover - simple init
                super().__init__(sleep_seconds=0.1)

            def enforce(self) -> None:  # type: ignore[override]
                limiter_calls.append(1)

        paginator = Paginator.from_config(
            {
                'type': PaginationType.PAGE,
                'page_size': 1,
                'records_path': 'items',
            },
            fetch=fetch,
            rate_limiter=DummyLimiter(),
        )

        records = list(paginator.paginate_iter('https://example.test/items'))

        assert [rec['id'] for rec in records] == [1, 2]
        assert len(limiter_calls) == 2

    def test_start_page_normalization(
        self,
        paginator_mode_case: tuple[str, int | None, int],
    ) -> None:
        """
        Test that ``start_page`` values are normalized by paginator type.
        """
        ptype, actual, expected = paginator_mode_case
        cfg: dict[str, Any] = {'type': ptype}
        if actual is not None:
            cfg['start_page'] = actual

        paginator = Paginator.from_config(cfg, fetch=_dummy_fetch)

        if ptype not in {'page', 'offset', 'cursor'}:
            assert paginator.type == 'page'
        else:
            assert paginator.type == ptype

        assert paginator.start_page == expected


class TestPaginatorInternalBranches:
    """Additional branch coverage for paginator internals."""

    def test_coalesce_records_fallback_missing_and_none_payload(self) -> None:
        """
        Test that missing fallback keeps ``None`` and triggers default payload
        path.
        """
        records = Paginator.coalesce_records(
            None,
            None,
            fallback_path='missing.path',
        )
        assert records == [{'value': None}]

    def test_coalesce_records_missing_fallback_and_dict_paths(self) -> None:
        """
        Test that :meth:`coalesce_records` covers missing/fallback/list/dict
        branches.
        """
        missing = Paginator.coalesce_records({'a': 1}, 'missing.path')
        assert missing == [{'value': None}]

        no_fallback = Paginator.coalesce_records(
            {'a': 1},
            None,
            fallback_path='missing.path',
        )
        assert no_fallback == [{'a': 1}]

        list_with_scalar = Paginator.coalesce_records([1, {'id': 2}], None)
        assert list_with_scalar == [{'value': 1}, {'id': 2}]

        dict_items = Paginator.coalesce_records({'items': [{'id': 3}]}, None)
        assert dict_items == [{'id': 3}]

        dict_plain = Paginator.coalesce_records({'id': 4}, None)
        assert dict_plain == [{'id': 4}]

    def test_cursor_iteration_stops_on_stop_limits_branch(self) -> None:
        """
        Test that cursor iterator break when stop limits are reached.
        """
        payloads = [
            {
                'records': [{'id': 1}],
                'meta': {'next': 'abc'},
            },
        ]

        def fetch(
            _url: str,
            _request: RequestOptions,
            _page: int | None,
        ) -> dict[str, Any]:
            return payloads[0]

        paginator = Paginator(
            type=PaginationType.CURSOR,
            fetch=fetch,
            records_path='records',
            cursor_path='meta.next',
            max_pages=1,
        )
        rows = list(paginator.paginate_iter('https://example.test/items'))
        assert rows == [{'id': 1}]

    def test_fetch_page_raises_when_fetch_missing(self) -> None:
        """Test that :meth:`_fetch_page` raises when callback is absent."""
        paginator = Paginator(fetch=None)
        with pytest.raises(ValueError, match='fetch must be provided'):
            paginator._fetch_page(
                'https://example.test/items',
                RequestOptions(),
            )

    def test_fetch_page_wraps_api_request_error(self) -> None:
        """
        Test that :class:`ApiRequestError` is wrapped as
        :class:`PaginationError`.
        """

        def failing_fetch(
            _url: str,
            _request: RequestOptions,
            _page: int | None,
        ) -> dict[str, Any]:
            raise ApiRequestError(url='https://example.test/items', status=429)

        paginator = Paginator(fetch=failing_fetch, type=PaginationType.PAGE)
        paginator.last_page = 4
        with pytest.raises(PaginationError):
            paginator._fetch_page(
                'https://example.test/items',
                RequestOptions(),
            )

    def test_from_config_accepts_pagination_config_instance(self) -> None:
        """
        Test that :meth:`from_config` handles :class:`PaginationConfig` objects
        directly.
        """
        cfg = PaginationConfig(
            type=PaginationType.PAGE,
            page_size=3,
            start_page=2,
            records_path='items',
        )
        paginator = Paginator.from_config(cfg, fetch=_dummy_fetch)
        assert paginator.page_size == 3
        assert paginator.start_page == 2
        assert paginator.records_path == 'items'

    def test_limit_batch_exhausted_branch(self) -> None:
        """
        Test that :meth:`_limit_batch` exhausts when emitted reaches
        *max_records*.
        """
        paginator = Paginator(fetch=_dummy_fetch, max_records=2)
        trimmed, exhausted = paginator._limit_batch([{'id': 1}], emitted=2)
        assert trimmed == []
        assert exhausted is True

    def test_next_cursor_from_invalid_paths(self) -> None:
        """
        Test that :meth:`next_cursor_from` returns ``None`` for invalid
        traversal inputs.
        """
        assert Paginator.next_cursor_from([], 'meta.next') is None
        assert (
            Paginator.next_cursor_from({'meta': 'oops'}, 'meta.next') is None
        )

    def test_paginate_iter_exits_when_type_is_unrecognized(self) -> None:
        """
        Test that an unknown runtime type falls through match and yields
        nothing.
        """
        paginator = Paginator(fetch=_dummy_fetch)
        paginator.type = cast(Any, 'unknown')
        assert not list(paginator.paginate_iter('https://example.test/items'))

    def test_paginate_iter_raises_when_fetch_missing(self) -> None:
        """
        Test that :meth:`paginate_iter` raises when fetch callback is missing.
        """
        paginator = Paginator(fetch=None)
        with pytest.raises(ValueError, match='fetch must be provided'):
            list(paginator.paginate_iter('https://example.test/items'))

    def test_post_init_normalizes_invalid_values(self) -> None:
        """
        Test that direct construction normalizes type/start/page_size/params.
        """
        paginator = Paginator(
            type=cast(Any, 'invalid'),
            start_page=-5,
            page_size=0,
            page_param='',
            size_param='',
            cursor_param='',
            limit_param='',
            fetch=_dummy_fetch,
        )
        assert paginator.type == PaginationType.PAGE
        assert paginator.start_page == 1
        assert paginator.page_size == 1
        assert paginator.page_param == 'page'
        assert paginator.size_param == 'per_page'
        assert paginator.cursor_param == PaginationType.CURSOR
        assert paginator.limit_param == 'limit'

    def test_post_init_normalizes_zero_start_page_and_keeps_limit_param(
        self,
    ) -> None:
        """
        Test that page ``start_page=0`` normalizes to 1 and non-empty
        *limit_param* is kept.
        """
        paginator = Paginator(
            type=PaginationType.PAGE,
            start_page=0,
            limit_param='lim',
            fetch=_dummy_fetch,
        )
        assert paginator.start_page == 1
        assert paginator.limit_param == 'lim'

    def test_resolve_start_page_invalid_and_negative_offset(self) -> None:
        """
        Test that start-page resolver handles invalid and negative overrides.
        """
        page = Paginator(fetch=_dummy_fetch, type=PaginationType.PAGE)
        out_page = page._resolve_start_page(
            RequestOptions(params={page.page_param: 'bad'}),
        )
        assert out_page == page.start_page

        offset = Paginator(fetch=_dummy_fetch, type=PaginationType.OFFSET)
        out_offset = offset._resolve_start_page(
            RequestOptions(params={offset.page_param: -5}),
        )
        assert out_offset == offset.START_PAGES[offset.type]

    def test_stop_limits_max_records_branch(self) -> None:
        """
        Test that :meth:`_stop_limits` returns ``True`` when record cap is
        reached.
        """
        paginator = Paginator(fetch=_dummy_fetch, max_records=2)
        assert paginator._stop_limits(pages=1, recs=2) is True
