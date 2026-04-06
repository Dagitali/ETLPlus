"""
:mod:`tests.unit.api.test_u_api_types` module.

Unit tests for :mod:`etlplus.api._types`.
"""

from __future__ import annotations

import pytest

from etlplus.api._types import FetchPageCallable
from etlplus.api._types import Headers
from etlplus.api._types import Params
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

    def test_request_options_defaults(self) -> None:
        """
        Test that :class:`RequestOptions` defaults all fields to ``None``.
        """
        opts = RequestOptions()

        assert opts.params is None
        assert opts.headers is None
        assert opts.timeout is None

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

    def test_request_options_invalid_params_headers(self) -> None:
        """
        Test that :class:`RequestOptions` coerces mapping-like params and
        headers to dicts.
        """

        # Should coerce mapping-like objects to dict.
        class DummyMap(dict):
            """Dummy mapping-like class for testing."""

        opts = RequestOptions(params=DummyMap(a=1), headers=DummyMap(X='y'))
        assert isinstance(opts.params, dict)
        assert isinstance(opts.headers, dict)

        cleared = RequestOptions(params=None, headers=None)
        assert cleared.params is None
        assert cleared.headers is None


class TestApiTypes:
    """Unit tests for public API type aliases."""

    def test_type_aliases_support_fetch_page_signature(self) -> None:
        """
        Test that :class:`FetchPageCallable` accepts the documented callback
        signature.
        """
        cb: FetchPageCallable = _fetch_page
        assert callable(cb)

    def test_type_aliases_edge_cases(self) -> None:
        """
        Test that type alias examples remain valid for typical edge case
        values.
        """
        url: Url = 'http://test/'
        headers: Headers = {'A': 'B'}
        params: Params = {'A': 1, 'B': [1, 2]}
        cb: FetchPageCallable = _fetch_page

        assert isinstance(url, str)
        assert isinstance(headers, dict)
        assert isinstance(params, dict)
        assert callable(cb)
