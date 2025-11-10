"""
tests.conftest.py test fixtures module.

Shared pytest fixtures for the test suite.

Provides reusable factory for `EndpointClient` and a sleep capture fixture
used by retry logic tests.
"""
from __future__ import annotations

import types
from os import PathLike
from pathlib import Path
from typing import Any
from typing import Callable
from typing import cast
from typing import TypedDict
from typing import Unpack

import pytest

from etlplus.api import CursorPaginationConfig
from etlplus.api import PagePaginationConfig
from etlplus.api.client import EndpointClient
from etlplus.enums import DataConnectorType
from tests.unit.api.test_mocks import MockSession


# SECTION: HELPERS ========================================================== #


class _CursorKw(TypedDict, total=False):
    cursor_param: str
    cursor_path: str
    page_size: int | str
    records_path: str
    start_cursor: str | int
    max_pages: int
    max_records: int


class _PageKw(TypedDict, total=False):
    page_param: str
    size_param: str
    start_page: int
    page_size: int
    records_path: str
    max_pages: int
    max_records: int


def _freeze(d: dict[str, Any]) -> types.MappingProxyType:  # pragma: no cover
    return types.MappingProxyType(d)


# SECTION: FIXTURES ========================================================= #


@pytest.fixture
def capture_sleeps(
    monkeypatch: pytest.MonkeyPatch,
) -> list[float]:
    """
    Capture applied sleep durations from retry backoff logic.

    Patches `EndpointClient.apply_sleep` so tests can assert jitter/backoff
    behavior without actually waiting.
    """
    values: list[float] = []

    def _sleep(s: float, *, _sleeper=None) -> None:  # noqa: D401, ANN001
        values.append(s)

    monkeypatch.setattr(
        EndpointClient,
        'apply_sleep',
        staticmethod(_sleep),
        raising=False,
    )

    return values


@pytest.fixture
def client_factory() -> Callable[..., EndpointClient]:
    """
    Return a factory to build `EndpointClient` instances.

    Parameters can be overridden per test. `endpoints` defaults to an empty
    mapping to simplify most calls.
    """
    def _make(
        *,
        base_url: str = 'https://api.example.com',
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
def cursor_cfg() -> Callable[..., CursorPaginationConfig]:
    """Builder for cursor pagination config (immutable).

    Returns a function: cursor_cfg(**kwargs) -> CursorPaginationConfig
    """

    def _make(**kwargs: Unpack[_CursorKw]) -> CursorPaginationConfig:
        base: dict[str, Any] = {'type': 'cursor'}
        base.update(kwargs)
        return cast(CursorPaginationConfig, _freeze(base))

    return _make


@pytest.fixture
def offset_cfg() -> Callable[..., PagePaginationConfig]:
    """Builder for offset pagination config (immutable)."""

    def _make(**kwargs: Unpack[_PageKw]) -> PagePaginationConfig:
        base: dict[str, Any] = {'type': 'offset'}
        base.update(kwargs)
        return cast(PagePaginationConfig, _freeze(base))

    return _make


@pytest.fixture
def extract_stub(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    """
    Patch EndpointClient's module-level _extract and capture calls.

    Returns dict with:
      urls: list[str]
      kwargs: list[dict[str, Any]]
    """
    import etlplus.api.client as cmod  # local import to avoid cycles

    calls: dict[str, Any] = {'urls': [], 'kwargs': []}

    def _fake_extract(kind: str, url: str, **kwargs: Any):  # noqa: D401
        assert kind == 'api'
        calls['urls'].append(url)
        calls['kwargs'].append(kwargs)
        return {'ok': True}

    monkeypatch.setattr(cmod, '_extract', _fake_extract)

    return calls


@pytest.fixture(scope='session')
def extract_stub_factory() -> Callable[..., Any]:
    """
    Provide a per-use stub factory for ``_extract`` without relying on
    function-scoped fixtures (Hypothesis-friendly).

    Usage in tests:

        with extract_stub_factory() as calls:
            client.paginate(...)
            assert calls['urls'] == [...]

    Each invocation patches ``etlplus.api.client._extract`` for the duration
    of the context manager and restores the original afterwards.
    """
    import contextlib
    import etlplus.api.client as cmod  # Local import to avoid cycles

    @contextlib.contextmanager
    def _make(
        *,
        return_value: Any | None = None,
    ):  # noqa: D401
        calls: dict[str, Any] = {'urls': [], 'kwargs': []}

        def _fake_extract(
            source_type: DataConnectorType | str,
            source: str | Path | PathLike[str],
            **kwargs: Any,
        ) -> dict[str, Any] | list[dict[str, Any]]:  # noqa: D401
            calls['urls'].append(str(source))
            calls['kwargs'].append({'source_type': source_type, **kwargs})
            return {'ok': True} if return_value is None else return_value

        saved = getattr(cmod, '_extract')
        cmod._extract = _fake_extract  # type: ignore[attr-defined]
        try:
            yield calls
        finally:
            cmod._extract = saved  # type: ignore[attr-defined]

    return _make


@pytest.fixture
def jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[list[float]], list[float]]:
    """
    Control retry jitter deterministically by supplying a sequence of values.

    Usage:
        vals = jitter([0.1, 0.2])
        # Now client jitter will use 0.1, then 0.2 for random.uniform(a, b)
    """
    import etlplus.api.client as cmod  # local import to avoid cycles

    def _set(values: list[float]) -> list[float]:
        seq = iter(values)
        monkeypatch.setattr(cmod.random, 'uniform', lambda a, b: next(seq))
        return values

    return _set


@pytest.fixture
def mock_session() -> MockSession:
    """
    Provide a fresh ``MockSession`` per test.

    Useful for tests that need to pass a raw session into ``EndpointClient``
    or verify close semantics.
    """
    return MockSession()


@pytest.fixture
def page_cfg() -> Callable[..., PagePaginationConfig]:
    """Builder for page-number pagination config (immutable)."""

    def _make(**kwargs: Unpack[_PageKw]) -> PagePaginationConfig:
        base: dict[str, Any] = {'type': 'page'}
        base.update(kwargs)
        return cast(PagePaginationConfig, _freeze(base))

    return _make


@pytest.fixture
def retry_cfg() -> Callable[..., dict[str, Any]]:
    """Factory for building retry configuration dictionaries."""

    def _make(**kwargs: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            'max_attempts': kwargs.pop('max_attempts', 3),
            'backoff': kwargs.pop('backoff', 0.0),
        }
        base.update(kwargs)
        return base

    return _make
