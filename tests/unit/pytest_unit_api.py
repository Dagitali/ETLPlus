"""
:mod:`tests.unit.pytest_unit_api` module.

Pytest plugin with API-oriented fixtures shared across unit tests.
"""

from __future__ import annotations

import types
from collections.abc import Callable
from typing import Any
from typing import TypedDict
from typing import Unpack
from typing import cast

import pytest
import requests  # type: ignore[import]

from etlplus.api import CursorPaginationConfigDict
from etlplus.api import EndpointClient
from etlplus.api import PagePaginationConfigDict
from tests.unit.api.test_u_api_mocks import MockSession

# SECTION: TYPES ============================================================ #


class _CursorKwDict(TypedDict, total=False):
    cursor_param: str
    cursor_path: str
    page_size: int | str
    records_path: str
    start_cursor: str | int
    max_pages: int
    max_records: int


class _PageKwDict(TypedDict, total=False):
    page_param: str
    size_param: str
    start_page: int
    page_size: int
    records_path: str
    max_pages: int
    max_records: int


# SECTION: HELPERS ========================================================== #


def _freeze(
    d: dict[str, Any],
) -> types.MappingProxyType:
    """
    Create an immutable, read-only mapping proxy for a dictionary.

    Parameters
    ----------
    d : dict[str, Any]
        Dictionary to freeze.

    Returns
    -------
    types.MappingProxyType
        Read-only mapping proxy of the input dictionary.
    """
    return types.MappingProxyType(d)


# SECTION: FIXTURES ========================================================= #


@pytest.fixture
def api_profile_defaults_factory() -> Callable[..., dict[str, Any]]:
    """
    Create a factory to build API profile defaults block dictionaries.

    Returns
    -------
    Callable[..., dict[str, Any]]
        Function that builds a profile defaults mapping for API config.

    Examples
    --------
    >>> defaults = api_profile_defaults_factory(
    ...     pagination={'type': 'page', 'page_param': 'p', 'size_param': 's'},
    ...     rate_limit={'sleep_seconds': 0.1, 'max_per_sec': 5},
    ...     headers={'X': '1'},
    ... )
    """

    def _make(
        *,
        pagination: dict[str, Any] | None = None,
        rate_limit: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if pagination is not None:
            out['pagination'] = pagination
        if rate_limit is not None:
            out['rate_limit'] = rate_limit
        if headers is not None:
            out['headers'] = headers
        return out

    return _make


@pytest.fixture
def client_factory(
    base_url: str,
) -> Callable[..., EndpointClient]:
    """
    Create a factory to build :class:`EndpointClient` instances.

    Parameters can be overridden per test. Endpoints default to an empty
    mapping for convenience.

    Parameters
    ----------
    base_url : str
        Common base URL used across tests.

    Returns
    -------
    Callable[..., EndpointClient]
        Function that builds :class:`EndpointClient` instances.
    """

    def _make(
        *,
        base_url: str = base_url,
        endpoints: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> EndpointClient:
        return EndpointClient(
            base_url=base_url,
            endpoints=endpoints or {},
            **kwargs,
        )

    return _make


@pytest.fixture
def cursor_cfg() -> Callable[..., CursorPaginationConfigDict]:
    """
    Create a factory for building immutable cursor pagination config objects.

    Returns
    -------
    Callable[..., CursorPaginationConfigDict]
        Function that builds :class:`CursorPaginationConfigDict` instances.
    """

    def _make(**kwargs: Unpack[_CursorKwDict]) -> CursorPaginationConfigDict:
        base: dict[str, Any] = {'type': 'cursor'}
        base.update(kwargs)
        return cast(CursorPaginationConfigDict, _freeze(base))

    return _make


@pytest.fixture
def request_once_stub(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    """
    Patch :meth:`EndpointClient.request_once` and capture calls.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture for patching.

    Returns
    -------
    dict[str, Any]
        Dictionary with:
            urls: list[str]
            kwargs: list[dict[str, Any]]
    """
    # pylint: disable=unused-argument

    # Locally import to avoid cycles.
    import etlplus.api.request_manager as rm_module

    calls: dict[str, Any] = {'urls': [], 'kwargs': []}

    def _fake_request(
        self: rm_module.RequestManager,
        method: str,
        url: str,
        *,
        session: Any,
        timeout: Any,
        request_callable: Callable[..., Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:  # noqa: D401
        assert method == 'GET'
        calls['urls'].append(url)
        calls['kwargs'].append(kwargs)
        return {'ok': True}

    monkeypatch.setattr(
        rm_module.RequestManager,
        'request_once',
        _fake_request,
    )

    return calls


@pytest.fixture(scope='session')
def extract_stub_factory() -> Callable[..., Any]:
    """
    Create a factory to build a per-use stub factory for patching
    the low-level HTTP helper without relying on function-scoped fixtures
    (Hypothesis-friendly).

    Each invocation patches
    :meth:`etlplus.api.request_manager.RequestManager.request_once` for the
    duration of the context manager and restores the original afterwards.

    Returns
    -------
    Callable[..., Any]
        Function that builds a call capture dictionary.

    Examples
    --------
    >>> with extract_stub_factory() as calls:
    ...     client.paginate(...)
    ...     assert calls['urls'] == [...]
    """
    # pylint: disable=unused-argument

    def _make(
        *,
        return_value: Any | None = None,
    ) -> Any:
        # pylint: disable=protected-access
        import contextlib

        import etlplus.api.request_manager as rm_module

        calls: dict[str, Any] = {'urls': [], 'kwargs': []}

        @contextlib.contextmanager
        def _cm() -> Any:
            original = rm_module.RequestManager.request_once

            def _fake_request(
                self: rm_module.RequestManager,
                method: str,
                url: str,
                *,
                session: Any,
                timeout: Any,
                request_callable: Callable[..., Any] | None = None,
                **kwargs: Any,
            ) -> dict[str, Any] | list[dict[str, Any]]:
                _ = method
                calls['urls'].append(url)
                calls['kwargs'].append(kwargs)
                return {'ok': True} if return_value is None else return_value

            monkeypatch = pytest.MonkeyPatch()
            monkeypatch.setattr(
                rm_module.RequestManager,
                'request_once',
                _fake_request,
            )
            try:
                yield calls
            finally:
                monkeypatch.setattr(
                    rm_module.RequestManager,
                    'request_once',
                    original,
                )

        return _cm()

    return _make


@pytest.fixture
def mock_session() -> MockSession:
    """
    Provide a reusable :class:`MockSession` fixture.

    Returns
    -------
    MockSession
        New mock session for each test.
    """
    return MockSession()


@pytest.fixture
def page_cfg() -> Callable[..., PagePaginationConfigDict]:
    """
    Create a factory for building immutable page pagination config objects.

    Returns
    -------
    Callable[..., PagePaginationConfigDict]
        Function that builds :class:`PagePaginationConfigDict` instances.
    """

    def _make(**kwargs: Unpack[_PageKwDict]) -> PagePaginationConfigDict:
        base: dict[str, Any] = {'type': 'page'}
        base.update(kwargs)
        return cast(PagePaginationConfigDict, _freeze(base))

    return _make


@pytest.fixture
def retry_cfg() -> Callable[..., dict[str, Any]]:
    """
    Create a factory for retry settings dictionaries.

    Returns
    -------
    Callable[..., dict[str, Any]]
        Function that builds retry settings dictionaries.
    """

    def _make(**kwargs: Any) -> dict[str, Any]:
        out = {
            'retry_statuses': [429, 500],
            'max_retries': 2,
            'initial_backoff': 0.01,
        }
        out.update(kwargs)
        return out

    return _make


@pytest.fixture
def token_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, int]:
    """
    Stub :func:`requests.post` and return call count holder for token flow.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.

    Returns
    -------
    dict[str, int]
        Mapping with key ``'n'`` incremented for each token call.
    """
    # pylint: disable=unused-argument

    calls = {'n': 0}

    def fake_post(
        *args: object,
        **kwargs: object,
    ) -> object:
        calls['n'] += 1
        # _Resp is defined in test_u_auth.py, so return a dict for generality.
        return {'access_token': f't{calls["n"]}', 'expires_in': 60}

    monkeypatch.setattr(requests, 'post', fake_post)

    return calls
