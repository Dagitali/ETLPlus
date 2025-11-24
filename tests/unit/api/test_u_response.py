"""
``tests.unit.api.test_u_response`` module.

Unit tests for :class:`etlplus.api.response.Paginator`.

Notes
-----
- Ensures non-positive and non-numeric inputs result in 0.0 seconds.
- Ensures non-positive and non-numeric inputs result in a disabled limiter.
- Verifies helper constructors and configuration-based construction.

Examples
--------
>>> pytest tests/unit/api/test_u_response.py
"""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from typing import cast
from typing import Mapping

import pytest

from etlplus.api.client import EndpointClient
from etlplus.api.response import PagePaginationConfig
from etlplus.api.response import PaginationConfig
from etlplus.api.response import PaginationType
from etlplus.api.response import Paginator


# SECTION: HELPERS ========================================================== #


def _dummy_fetch(
    url: str,
    params: Mapping[str, Any] | None,
    page: int | None,
) -> Mapping[str, Any]:
    """Simple fetch stub that echoes input for Paginator construction."""
    return {'url': url, 'params': params or {}, 'page': page}


class RecordingClient(EndpointClient):
    """EndpointClient subclass that records paginate_url_iter calls.

    Used to verify that ``paginate`` and ``paginate_iter`` are thin shims
    over ``paginate_url_iter``.
    """

    _paginate_calls: list[dict[str, Any]] = []

    @property
    def paginate_calls(self) -> list[dict[str, Any]]:
        return type(self)._paginate_calls

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        type(self)._paginate_calls.clear()

    def paginate_url_iter(
        self,
        url: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, Any] | None,
        timeout: float | int | None,
        pagination: PaginationConfig | None,
        *,
        sleep_seconds: float = 0.0,
    ) -> Iterator[dict]:
        """Record arguments and yield a single marker record."""
        type(self)._paginate_calls.append(
            {
                'url': url,
                'params': params,
                'headers': headers,
                'timeout': timeout,
                'pagination': pagination,
                'sleep_seconds': sleep_seconds,
            },
        )
        yield {'marker': 'ok'}


class FakePageClient(EndpointClient):
    def paginate_url_iter(
        self,
        url: str,
        params: Mapping[str, Any] | None,
        headers: Mapping[str, Any] | None,
        timeout: float | int | None,
        pagination: PaginationConfig | None,
        *,
        sleep_seconds: float = 0.0,
    ) -> Iterator[dict]:
        # Ignore all arguments; just simulate three records from two pages.
        yield {'id': 1}
        yield {'id': 2}
        yield {'id': 3}


# SECTION: TESTS ============================================================ #


class TestPaginator:
    """Tests for :class:`Paginator` configuration and normalization logic."""

    def test_defaults_when_missing_keys(self) -> None:
        """
        Confirm that default parameter names and limits are preserved.

        Notes
        -----
        When optional pagination configuration keys are omitted, the
        paginator should fall back to its class-level defaults.
        """
        cfg: PagePaginationConfig = {'type': PaginationType.PAGE}

        paginator = Paginator.from_config(cfg, fetch=_dummy_fetch)

        assert paginator.page_param == \
            Paginator.PAGE_PARAMS[PaginationType.PAGE]
        assert paginator.size_param == \
            Paginator.SIZE_PARAMS[PaginationType.PAGE]
        assert paginator.limit_param == Paginator.LIMIT_PARAM
        assert paginator.cursor_param == Paginator.CURSOR_PARAM
        assert paginator.records_path is None
        assert paginator.cursor_path is None
        assert paginator.max_pages is None
        assert paginator.max_records is None
        assert paginator.start_cursor is None

    @pytest.mark.parametrize(
        'actual, expected',
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
        expected: int,
    ) -> None:
        """
        Ensure ``page_size`` values are coerced to a positive integer.

        Parameters
        ----------
        actual : int | None
            Raw configured page size.
        expected : int
            Expected normalized page size.
        """
        cfg: PagePaginationConfig = {'type': PaginationType.PAGE}
        if actual is not None:
            cfg['page_size'] = actual

        paginator = Paginator.from_config(cfg, fetch=_dummy_fetch)
        assert paginator.page_size == expected

    @pytest.mark.parametrize(
        'ptype, actual, expected_start',
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
        Verify that ``start_page`` values are normalized by paginator type.

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

    def test_page_integration(self) -> None:
        """Exercise paginate over a multi-record iterator.

        Uses a lightweight EndpointClient subclass that overrides
        ``paginate_url_iter`` to simulate multiple pages of results and
        verifies that ``paginate`` flattens them into a single record stream.
        """

        client = FakePageClient(
            base_url='https://example.test/api',
            endpoints={'items': '/items'},
        )

        pg: PagePaginationConfig = {
            'type': PaginationType.PAGE,
            'page_param': 'page',
            'size_param': 'per_page',
            'page_size': 2,
            'records_path': 'items',
        }

        records = cast(
            list[dict[str, Any]],
            list(client.paginate('items', pagination=pg)),
        )

        assert [r['id'] for r in records] == [1, 2, 3]

    def test_paginate_and_paginate_iter_are_thin_shims(self) -> None:
        """Ensure paginate and paginate_iter delegate to paginate_url_iter."""
        client = RecordingClient(
            base_url='https://example.test/api',
            endpoints={'items': '/items'},
        )

        pg: PagePaginationConfig = {'type': PaginationType.PAGE}

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
