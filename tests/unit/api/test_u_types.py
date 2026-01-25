"""
:mod:`tests.unit.api.test_u_types` module.

Unit tests for :mod:`etlplus.api.types`.
"""

from __future__ import annotations

import pytest

from etlplus.api.types import FetchPageCallable
from etlplus.api.types import Headers
from etlplus.api.types import Params
from etlplus.api.types import RequestOptions
from etlplus.api.types import Url

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    'opts, expected',
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
    opts: RequestOptions,
    expected: dict[str, object],
) -> None:
    """
    Test that :meth:`RequestOptions.as_kwargs` produces the expected dict.
    """
    assert opts.as_kwargs() == expected


def test_request_options_defaults():
    """Test that :class:`RequestOptions` defaults to None fields."""
    opts = RequestOptions()
    assert opts.params is None
    assert opts.headers is None
    assert opts.timeout is None


@pytest.mark.parametrize(
    'kwargs, expected_params, expected_headers, expected_timeout',
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
    kwargs: dict[str, object],
    expected_params: dict[str, int] | None,
    expected_headers: dict[str, str] | None,
    expected_timeout: float | None,
) -> None:
    """Test :meth:`RequestOptions.evolve` variants for preserving/clearing."""
    opts = RequestOptions(params={'a': 1}, headers={'X': 'y'}, timeout=5.0)
    evolved = opts.evolve(**kwargs)
    assert evolved.params == expected_params
    assert evolved.headers == expected_headers
    assert evolved.timeout == expected_timeout


def test_request_options_invalid_params_headers():
    """
    Test that :class:`RequestOptions` coerces mapping-like objects to dict.
    """

    # Should coerce mapping-like objects to dict.
    class DummyMap(dict):
        """Dummy mapping-like class for testing."""

    opts = RequestOptions(params=DummyMap(a=1), headers=DummyMap(X='y'))
    assert isinstance(opts.params, dict)
    assert isinstance(opts.headers, dict)

    # Should handle None gracefully.
    opts2 = RequestOptions(params=None, headers=None)
    assert opts2.params is None
    assert opts2.headers is None


def test_type_aliases():
    """Test that type aliases are correct."""
    # pylint: disable=unused-argument

    # url: Url = 'https://api.example.com/data'
    # headers: Headers = {'Authorization': 'token'}
    # params: Params = {'q': 'search'}

    def fetch(
        url: Url,
        opts: RequestOptions,
        page: int | None,
    ) -> dict[str, list[int]]:
        """Return a payload to satisfy the callback signature."""
        return {'data': [1, 2, 3]}

    cb: FetchPageCallable = fetch
    assert callable(cb)


def test_type_aliases_edge_cases():
    """Test type aliases with edge case values."""
    # pylint: disable=unused-argument

    # Url must be str.
    url: Url = 'http://test/'
    assert isinstance(url, str)
    # Headers must be dict[str, str].
    headers: Headers = {'A': 'B'}
    assert isinstance(headers, dict)
    # Params must be dict[str, Any].
    params: Params = {'A': 1, 'B': [1, 2]}
    assert isinstance(params, dict)

    # FetchPageCallable must accept correct signature.
    def fetch(
        url: Url,
        opts: RequestOptions,
        page: int | None,
    ) -> dict[str, list[int]]:
        """Return a payload to satisfy the callback signature."""
        return {'data': []}

    cb: FetchPageCallable = fetch
    assert callable(cb)
