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

from typing import Any
from typing import Mapping

import pytest

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
        type_ = PaginationType.PAGE
        cfg: PaginationConfig = {'type': type_}

        paginator = Paginator.from_config(cfg, fetch=_dummy_fetch)

        assert paginator.page_param == Paginator.PAGE_PARAMS[type_]
        assert paginator.size_param == Paginator.SIZE_PARAMS[type_]
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
        cfg: PaginationConfig = {'type': 'page'}
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
        expected_start: int,
    ) -> None:
        """
        Verify that ``start_page`` values are normalized by paginator type.

        Parameters
        ----------
        ptype : str
            Raw pagination type from configuration.
        actual : int | None
            Configured start page value.
        expected_start : int
            Expected normalized start page value.
        """
        cfg: PaginationConfig = {'type': ptype}
        if actual is not None:
            cfg['start_page'] = actual

        paginator = Paginator.from_config(cfg, fetch=_dummy_fetch)

        if ptype not in {'page', 'offset', 'cursor'}:
            assert paginator.type == 'page'
        else:
            assert paginator.type == ptype

        assert paginator.start_page == expected_start
