"""
:mod:`tests.unit.api.test_u_types` module.

Unit tests for :mod:`etlplus.api.types`.
"""

import pytest

from etlplus.api.types import FetchPageCallable
from etlplus.api.types import Headers
from etlplus.api.types import Params
from etlplus.api.types import RequestOptions
from etlplus.api.types import Url

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


def test_request_options_as_kwargs():
    """Test that :meth:`RequestOptions.as_kwargs` produces correct dict."""
    opts = RequestOptions(params={'a': 1}, headers={'X': 'y'}, timeout=5.0)
    kw = opts.as_kwargs()
    assert kw['params'] == {'a': 1}
    assert kw['headers'] == {'X': 'y'}
    assert kw['timeout'] == 5.0


def test_request_options_as_kwargs_edge_cases():
    """Test :meth:`RequestOptions.as_kwargs` with unset and ``None`` fields."""
    opts = RequestOptions()
    kw = opts.as_kwargs()
    assert not kw

    opts2 = RequestOptions(params={'x': 1})
    kw2 = opts2.as_kwargs()
    assert kw2['params'] == {'x': 1}
    assert 'headers' not in kw2
    assert 'timeout' not in kw2


def test_request_options_defaults():
    """Test that :class:`RequestOptions` defaults to None fields."""
    opts = RequestOptions()
    assert opts.params is None
    assert opts.headers is None
    assert opts.timeout is None


def test_request_options_evolve():
    """
    Test that :meth:`RequestOptions.evolve` creates modified copies correctly.
    """
    opts = RequestOptions(params={'a': 1}, headers={'X': 'y'}, timeout=5.0)
    evolved = opts.evolve(params={'b': 2}, headers=None, timeout=None)
    assert evolved.params == {'b': 2}
    assert evolved.headers is None
    assert evolved.timeout is None


def test_request_options_evolve_edge_cases():
    """Test :meth:`RequestOptions.evolve` with unset and ``None`` fields."""
    opts = RequestOptions(params={'a': 1}, headers={'X': 'y'}, timeout=5.0)

    # Evolve with _UNSET (should preserve existing).
    evolved = opts.evolve()
    assert evolved.params == {'a': 1}
    assert evolved.headers == {'X': 'y'}
    assert evolved.timeout == 5.0

    # Evolve with None (should clear).
    evolved2 = opts.evolve(params=None, headers=None, timeout=None)
    assert evolved2.params is None
    assert evolved2.headers is None
    assert evolved2.timeout is None


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

    def fetch(url: Url, opts: RequestOptions, page: int | None):
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
    def fetch(url: Url, opts: RequestOptions, page: int | None):
        return {'data': []}

    cb: FetchPageCallable = fetch
    assert callable(cb)
