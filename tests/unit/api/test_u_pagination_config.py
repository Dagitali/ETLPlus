"""
:mod:`tests.unit.api.test_u_pagination_config` module.

Unit tests for :class:`etlplus.api.PaginationConfig`.

Notes
-----
- Parametrized across page, offset, and cursor pagination styles.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from typing import Literal

import pytest

from etlplus.api import PaginationConfig

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit

# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestPaginationConfig:
    """
    Unit test suite for :class:`PaginationConfig`.

    Notes
    -----
    Tests validation and parsing of pagination configuration for different
    pagination styles.
    """

    def test_defaults_apply_response_fallback_path(self) -> None:
        """Defaults mapping should surface response fallback_path."""
        cfg = PaginationConfig.from_defaults(
            {
                'type': 'page',
                'response': {
                    'items_path': 'data.items',
                    'fallback_path': 'payload.records',
                },
            },
        )

        assert cfg is not None
        assert cfg.records_path == 'data.items'
        assert cfg.fallback_path == 'payload.records'

    def test_defaults_preserve_top_level_fallback_path(self) -> None:
        """
        Nested fallback_path should not override explicit top-level value.
        """
        cfg = PaginationConfig.from_defaults(
            {
                'fallback_path': 'top.level.records',
                'response': {'fallback_path': 'ignored.path'},
                'params': {'fallback_path': 'ignored.params'},
            },
        )

        assert cfg is not None
        assert cfg.fallback_path == 'top.level.records'

    def test_from_obj_coerces_numeric_fields(
        self,
        pagination_from_obj_factory: Callable[[Any], PaginationConfig],
    ) -> None:
        """
        Test that from_obj coerces numeric fields correctly.

        Parameters
        ----------
        pagination_from_obj_factory : Callable[[Any], PaginationConfig]
            Factory for PaginationConfig.
        """
        obj = {
            'type': 'page',
            'page_param': 'page',
            'size_param': 'per_page',
            'start_page': '1',
            'page_size': '50',
            'records_path': 'data.items',
            'max_pages': '10',
            'max_records': '1000',
        }
        pc = pagination_from_obj_factory(obj)
        assert pc is not None
        assert pc.type == 'page'
        assert pc.start_page == 1
        assert pc.page_size == 50
        assert pc.max_pages == 10
        assert pc.max_records == 1000

    def test_from_obj_ignores_bad_numeric_values(
        self,
        pagination_from_obj_factory: Callable[[Any], PaginationConfig],
    ) -> None:
        """
        Test that from_obj ignores bad numeric values.

        Parameters
        ----------
        pagination_from_obj_factory : Callable[[Any], PaginationConfig]
            Factory for PaginationConfig.
        """
        obj: dict[str, Any] = {
            'type': 'page',
            'start_page': 'not-an-int',
            'page_size': None,
            'max_pages': [],
            'max_records': {},
        }
        pc = pagination_from_obj_factory(obj)
        assert pc is not None
        assert pc.start_page is None
        assert pc.page_size is None
        assert pc.max_pages is None
        assert pc.max_records is None

    def test_offset_mode_warnings(
        self,
        pagination_config_factory: Callable[..., PaginationConfig],
    ) -> None:
        """
        Test that offset mode warnings are produced correctly.

        Parameters
        ----------
        pagination_config_factory : Callable[..., PaginationConfig]
            Factory for PaginationConfig.
        """
        pc = pagination_config_factory(
            type='offset',
            start_page=0,
            page_size=-1,
        )
        warnings = pc.validate_bounds()
        assert 'start_page should be >= 1' in warnings
        assert 'page_size should be > 0' in warnings

    @pytest.mark.parametrize(
        'tval',
        [None, 'weird', ''],
        ids=['none', 'weird', 'empty'],
    )
    def test_unknown_type_general_warnings_only(
        self,
        tval: str | None,
        pagination_config_factory: Callable[..., Any],
    ) -> None:
        """
        Test that unknown pagination types only yield general warnings.

        Parameters
        ----------
        tval : str | None
            Pagination type value to test.
        pagination_config_factory : Callable[..., Any]
            Factory for PaginationConfig.
        """
        pc = pagination_config_factory(
            type=tval,
            start_page=0,
            page_size=0,
            max_pages=0,
            max_records=-1,
        )
        warnings = pc.validate_bounds()

        # General warnings should be present.
        assert 'max_pages should be > 0' in warnings
        assert 'max_records should be > 0' in warnings

        # No page/offset or cursor-specific warnings for unknown types.
        assert not any('start_page should be >= 1' in w for w in warnings)
        assert not any(
            'page_size should be > 0 for cursor pagination' in w
            for w in warnings
        )
        assert not any('page_size should be > 0' in w for w in warnings)

        pc2 = pagination_config_factory(
            type='offset',
            start_page=0,
            page_size=-1,
        )
        warnings2 = pc2.validate_bounds()
        assert 'start_page should be >= 1' in warnings2
        assert 'page_size should be > 0' in warnings2

    @pytest.mark.parametrize(
        'ptype',
        ['page', 'offset', 'cursor'],
        ids=['page', 'offset', 'cursor'],
    )
    def test_validate_bounds_parametrized(
        self,
        ptype: Literal['page', 'offset', 'cursor'],
        pagination_config_factory: Callable[..., PaginationConfig],
    ) -> None:
        """
        Test that validate_bounds produces correct warnings for different
        pagination types.

        Parameters
        ----------
        ptype : Literal['page', 'offset', 'cursor']
            Pagination type to test.
        pagination_config_factory : Callable[..., PaginationConfig]
            Factory for PaginationConfig.
        """
        pc = pagination_config_factory(
            type=ptype,
            start_page=0,
            page_size=0,
            max_pages=0,
            max_records=-1,
        )
        warnings = pc.validate_bounds()

        # General warnings should always appear.
        assert 'max_pages should be > 0' in warnings
        assert 'max_records should be > 0' in warnings

        if ptype in {'page', 'offset'}:
            assert 'start_page should be >= 1' in warnings
            assert 'page_size should be > 0' in warnings
            assert not any(
                'page_size should be > 0 for cursor pagination' in w
                for w in warnings
            )
        else:  # cursor
            assert not any('start_page should be >= 1' in w for w in warnings)
            assert any(
                'page_size should be > 0 for cursor pagination' in w
                for w in warnings
            )

    def test_valid_values_no_warnings(
        self,
        pagination_config_factory: Callable[..., PaginationConfig],
    ) -> None:  # noqa: D401
        """
        Test that valid pagination values produce no warnings.

        Parameters
        ----------
        pagination_config_factory : Callable[..., PaginationConfig]
            Factory for PaginationConfig.
        """
        pc = pagination_config_factory(
            type='page',
            start_page=1,
            page_size=10,
            max_pages=5,
            max_records=100,
        )
        assert pc.validate_bounds() == []
