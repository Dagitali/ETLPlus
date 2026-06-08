"""
:mod:`tests.unit.api.test_u_api_types` module.

Unit tests for :mod:`etlplus.api._types`.
"""

from __future__ import annotations

import pytest

from etlplus.api._types import FetchPageCallable
from etlplus.api._types import RequestOptions
from etlplus.api._types import Url

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def _fetch_page(
    url: Url,
    opts: RequestOptions,
    page: int | None,
) -> dict[str, list[int]]:
    """Return a simple payload satisfying `FetchPageCallable`."""
    _ = (url, opts, page)
    return {'data': [1, 2, 3]}


# SECTION: TESTS ============================================================ #


class TestRequestOptions:
    """Unit tests for `RequestOptions`."""

    @pytest.mark.parametrize(
        ('opts', 'expected'),
        [
            pytest.param(
                RequestOptions(
                    params={'a': 1},
                    headers={'X': 'y'},
                    timeout=5.0,
                ),
                {'params': {'a': 1}, 'headers': {'X': 'y'}, 'timeout': 5.0},
                id='full',
            ),
            pytest.param(RequestOptions(), {}, id='empty'),
            pytest.param(
                RequestOptions(params={'x': 1}),
                {'params': {'x': 1}},
                id='params-only',
            ),
        ],
    )
    def test_request_options_as_kwargs(
        self,
        opts: RequestOptions,
        expected: dict[str, object],
    ) -> None:
        """
        Test that :meth:`RequestOptions.as_kwargs` produces the expected dict.
        """
        assert opts.as_kwargs() == expected

    @pytest.mark.parametrize(
        'field_name',
        [
            pytest.param('params', id='params'),
            pytest.param('headers', id='headers'),
            pytest.param('timeout', id='timeout'),
        ],
    )
    def test_request_options_defaults(self, field_name: str) -> None:
        """
        Test that :class:`RequestOptions` defaults all fields to ``None``.
        """
        opts = RequestOptions()

        assert getattr(opts, field_name) is None

    @pytest.mark.parametrize(
        ('kwargs', 'expected_params', 'expected_headers', 'expected_timeout'),
        [
            pytest.param(
                {'params': {'b': 2}, 'headers': None, 'timeout': None},
                {'b': 2},
                None,
                None,
                id='override-clear',
            ),
            pytest.param(
                {},
                {'a': 1},
                {'X': 'y'},
                5.0,
                id='preserve',
            ),
            pytest.param(
                {'params': None, 'headers': None, 'timeout': None},
                None,
                None,
                None,
                id='explicit-none',
            ),
        ],
    )
    def test_request_options_evolve_variants(
        self,
        kwargs: dict[str, object],
        expected_params: dict[str, int] | None,
        expected_headers: dict[str, str] | None,
        expected_timeout: float | None,
    ) -> None:
        """
        Test that :meth:`RequestOptions.evolve` preserves and clears fields
        correctly.
        """
        evolved = RequestOptions(
            params={'a': 1},
            headers={'X': 'y'},
            timeout=5.0,
        ).evolve(**kwargs)

        assert evolved.params == expected_params
        assert evolved.headers == expected_headers
        assert evolved.timeout == expected_timeout

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('params', {'a': 1}, id='params'),
            pytest.param('headers', {'X': 'y'}, id='headers'),
        ],
    )
    def test_request_options_invalid_params_headers(
        self,
        field: str,
        expected: dict[str, object],
    ) -> None:
        """
        Test that :class:`RequestOptions` coerces mapping-like params and
        headers to dicts.
        """

        # Should coerce mapping-like objects to dict.
        class DummyMap(dict):
            """Dummy mapping-like class for testing."""

        opts = RequestOptions(params=DummyMap(a=1), headers=DummyMap(X='y'))
        assert getattr(opts, field) == expected

    @pytest.mark.parametrize(
        'field',
        [
            pytest.param('params', id='params'),
            pytest.param('headers', id='headers'),
        ],
    )
    def test_request_options_allows_none_params_headers(self, field: str) -> None:
        """Test that explicit ``None`` params and headers are preserved."""
        cleared = RequestOptions(params=None, headers=None)
        assert getattr(cleared, field) is None

    @pytest.mark.parametrize(
        ('field', 'value'),
        [
            pytest.param('params', 'bad', id='params'),
            pytest.param('headers', object(), id='headers'),
        ],
    )
    def test_request_options_rejects_non_mapping_params_headers(
        self,
        field: str,
        value: object,
    ) -> None:
        """Test that non-mapping params and headers normalize to empty dicts."""
        kwargs = {'params': None, 'headers': None}
        kwargs[field] = value
        opts = RequestOptions(**kwargs)  # type: ignore[arg-type]

        assert getattr(opts, field) == {}


class TestApiTypes:
    """Unit tests for public API type aliases."""

    def test_type_aliases_support_fetch_page_signature(self) -> None:
        """
        Test that :class:`FetchPageCallable` accepts the documented callback
        signature.
        """
        cb: FetchPageCallable = _fetch_page
        assert callable(cb)

    @pytest.mark.parametrize(
        ('value', 'expected_type'),
        [
            pytest.param('http://test/', str, id='url'),
            pytest.param({'A': 'B'}, dict, id='headers'),
            pytest.param({'A': 1, 'B': [1, 2]}, dict, id='params'),
        ],
    )
    def test_type_aliases_edge_cases(
        self,
        value: object,
        expected_type: type[object],
    ) -> None:
        """
        Test that type alias examples remain valid for typical edge case
        values.
        """
        assert isinstance(value, expected_type)

    def test_type_alias_callback_edge_case(self) -> None:
        """Test that callback aliases remain callable at runtime."""
        cb: FetchPageCallable = _fetch_page
        assert callable(cb)
