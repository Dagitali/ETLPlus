"""
tests.conftest.py test fixtures module.

Shared pytest fixtures for the test suite.

Provides reusable factory for `EndpointClient` and a sleep capture fixture
used by retry logic tests.
"""
from __future__ import annotations

import types
from typing import Any
from typing import Callable
from typing import cast
from typing import TypedDict
from typing import Unpack

import pytest

from etlplus.api import CursorPaginationConfig
from etlplus.api import PagePaginationConfig
from etlplus.api.client import EndpointClient


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
def page_cfg() -> Callable[..., PagePaginationConfig]:
    """Builder for page-number pagination config (immutable)."""

    def _make(**kwargs: Unpack[_PageKw]) -> PagePaginationConfig:
        base: dict[str, Any] = {'type': 'page'}
        base.update(kwargs)
        return cast(PagePaginationConfig, _freeze(base))

    return _make
