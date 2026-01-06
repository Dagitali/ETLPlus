"""
:mod:`tests.unit.api.test_u_paginator` module.

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
from etlplus.api import RateLimiter
from etlplus.api import RequestOptions
from etlplus.api.pagination import PagePaginationConfigMap
from etlplus.api.pagination import PaginationInput
from etlplus.api.pagination import PaginationType
from etlplus.api.pagination import Paginator
from etlplus.api.rate_limiting import RateLimitConfigMap

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

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
        rate_limit_overrides: RateLimitConfigMap | None = None,
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
        rate_limit_overrides: RateLimitConfigMap | None = None,
    ) -> Iterator[dict[str, Any]]:
        # Ignore all arguments; just simulate three records from two pages.
        _ = request  # keep signature compatibility while avoiding unused var
        yield {'id': 1}
        yield {'id': 2}
        yield {'id': 3}


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestPaginator:
    """Unit test suite for :class:`Paginator`."""

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
        Confirm that default parameter names and limits are preserved.

        Notes
        -----
        - When optional pagination configuration keys are omitted, the
            paginator should fall back to its class-level defaults.
        """
        cfg: PagePaginationConfigMap = {'type': PaginationType.PAGE}

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

        pg: PagePaginationConfigMap = {
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
        'actual, expected_page_size',
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

        Parameters
        ----------
        actual : int | None
            Raw configured page size.
        expected_page_size : int
            Expected normalized page size.
        """
        cfg: PagePaginationConfigMap = {'type': PaginationType.PAGE}
        if actual is not None:
            cfg['page_size'] = actual

        paginator = Paginator.from_config(cfg, fetch=_dummy_fetch)
        assert paginator.page_size == expected_page_size

    def test_paginate_accepts_request_options(self) -> None:
        """Paginator.paginate accepts RequestOptions overrides for params."""
        # pylint: disable=unused-argument

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
        # pylint: disable=protected-access

        client = RecordingClient(
            base_url='https://example.test/api',
            endpoints={'items': '/items'},
        )

        pg: PagePaginationConfigMap = {'type': PaginationType.PAGE}

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

    @pytest.mark.parametrize(
        'ptype, actual, expected',
        [
            ('page', None, 1),
            ('page', -5, 1),
            ('page', 0, 1),
            ('page', 3, 3),
            ('offset', None, 0),
            ('offset', -5, 0),
            ('offset', 0, 0),
            ('offset', 10, 10),
            ('bogus', 7, 7),  # falls back to ``"page"`` type
        ],
    )
    def test_start_page_normalization(
        self,
        ptype: str,
        actual: int | None,
        expected: int,
    ) -> None:
        """
        Test that ``start_page`` values are normalized by paginator type.

        Parameters
        ----------
        ptype : str
            Raw pagination type from configuration.
        actual : int | None
            Configured start page value.
        expected : int
            Expected normalized start page value.
        """
        cfg: dict[str, Any] = {'type': ptype}
        if actual is not None:
            cfg['start_page'] = actual

        paginator = Paginator.from_config(cfg, fetch=_dummy_fetch)

        if ptype not in {'page', 'offset', 'cursor'}:
            assert paginator.type == 'page'
        else:
            assert paginator.type == ptype

        assert paginator.start_page == expected
