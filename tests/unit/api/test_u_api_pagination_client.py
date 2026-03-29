"""
:mod:`tests.unit.api.test_u_api_pagination_client` module.

Unit tests for :class:`etlplus.api.pagination.PaginationClient`.

Focus
------
- Confirms pagination type detection is re-run when the config mutates.
- Exercises the single-page fallback path honoring ``records_path``.
- Verifies that post-mutation iteration delegates into :class:`Paginator`.
"""

from __future__ import annotations

from typing import Any

from etlplus.api import RequestOptions
from etlplus.api.pagination import PaginationClient
from etlplus.api.pagination import PaginationConfig
from etlplus.api.pagination import PaginationType

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestPaginationClient:
    """Unit tests targeting ``PaginationClient`` detection behavior."""

    @staticmethod
    def _noop_fetch(
        _url: str,
        _request: RequestOptions,
        _page: int | None,
    ) -> dict[str, Any]:
        """Return an empty payload for pagination client construction."""
        return {}

    def test_collect_and_single_page_with_pagination_config(self) -> None:
        """
        Test that :meth:`collect` and non-paginated single-page branches both
        work.
        """
        pagination = PaginationConfig(
            type=PaginationType.PAGE,
            records_path='payload.items',
        )
        typed_client = PaginationClient(
            pagination=pagination,
            fetch=self._noop_fetch,
        )
        assert typed_client.pagination_type == PaginationType.PAGE

        seen_pages: list[int | None] = []

        def fetch(
            _url: str,
            _request: RequestOptions,
            page: int | None,
        ) -> dict[str, Any]:
            seen_pages.append(page)
            return {'items': [{'id': 9}]}

        client = PaginationClient(
            pagination=None,
            fetch=fetch,
        )

        rows = client.collect('https://example.test/collect')
        assert rows == [{'id': 9}]
        assert seen_pages == [None]

    def test_is_paginated_reflects_config_mutations(self) -> None:
        """Test that pagination type detection re-runs on config mutation."""
        cfg: dict[str, Any] = {}
        client = PaginationClient(pagination=cfg, fetch=self._noop_fetch)

        assert client.pagination_type is None
        assert client.is_paginated is False

        cfg.update({'type': 'page', 'page_size': 1})

        assert client.is_paginated is True
        assert client.pagination_type == PaginationType.PAGE

    def test_iterate_single_page_uses_records_path(self) -> None:
        """
        Test that single-page fallback still extracts records via dot path.
        """
        payload = {'payload': {'items': [{'id': 1}, {'id': 2}]}}
        seen_pages: list[int | None] = []

        def fetch(
            _url: str,
            _request: RequestOptions,
            page: int | None,
        ) -> dict[str, Any]:
            seen_pages.append(page)
            return payload

        client = PaginationClient(
            pagination={'records_path': 'payload.items'},
            fetch=fetch,
        )

        rows = list(client.iterate('https://example.test'))

        assert rows == [{'id': 1}, {'id': 2}]
        assert seen_pages == [None]

    def test_iterate_after_mutation_uses_paginator(self) -> None:
        """
        Test that mutating the config to add ``type`` enables paginator
        iteration.
        """
        cfg: dict[str, Any] = {
            'records_path': 'items',
            'page_size': 1,
        }
        pages: dict[int, dict[str, list[dict[str, int]]]] = {
            1: {'items': [{'id': 1}]},
            2: {'items': [{'id': 2}]},
            3: {'items': []},
        }
        seen_pages: list[int | None] = []

        def fetch(
            _url: str,
            _request: RequestOptions,
            page: int | None,
        ) -> dict[str, Any]:
            seen_pages.append(page)
            idx = page or 1
            return pages[idx]

        client = PaginationClient(pagination=cfg, fetch=fetch)

        cfg['type'] = 'page'

        rows = list(client.iterate('https://example.test/items'))

        assert rows == [{'id': 1}, {'id': 2}]
        assert seen_pages[:3] == [1, 2, 3]
        assert client.pagination_type == PaginationType.PAGE

    def test_iterate_allows_request_overrides(self) -> None:
        """
        Test that explicit request snapshots can be supplied per invocation.
        """
        payload = {'payload': {'items': [{'id': 1}]}}
        captured: list[RequestOptions] = []

        def fetch(
            _url: str,
            request: RequestOptions,
            page: int | None,
        ) -> dict[str, Any]:
            captured.append(request)
            return payload

        client = PaginationClient(
            pagination={'records_path': 'payload.items'},
            fetch=fetch,
        )

        seed = RequestOptions(headers={'X-Seed': '1'}, timeout=5)
        call_request = seed.evolve(params={'page': 9})
        rows = list(
            client.iterate(
                'https://example.test/items',
                request=call_request,
            ),
        )

        assert rows == [{'id': 1}]
        assert len(captured) == 1
        assert captured[0].headers == {'X-Seed': '1'}
        assert captured[0].params == {'page': 9}
        assert captured[0].timeout == 5

    def test_iterate_with_paginator_respects_request_snapshot(self) -> None:
        """
        Test that paginator-backed iterations clone the provided
        :class:`RequestOptions`.
        """
        cfg = {
            'type': 'page',
            'records_path': 'items',
            'page_size': 1,
        }
        observed: list[RequestOptions] = []

        def fetch(
            _url: str,
            request: RequestOptions,
            page: int | None,
        ) -> dict[str, Any]:
            observed.append(request)
            return {'items': []}

        client = PaginationClient(pagination=cfg, fetch=fetch)
        request = RequestOptions(params={'seed': '1'}, headers={'A': 'B'})
        call_request = request.evolve(params={'page': 2})

        list(
            client.iterate(
                'https://example.test/items',
                request=call_request,
            ),
        )

        assert observed
        first = observed[0]
        assert first.params is not None
        assert first.params.get('page') == 2
        assert first.params.get('per_page') == cfg['page_size']
        assert first.headers == {'A': 'B'}
