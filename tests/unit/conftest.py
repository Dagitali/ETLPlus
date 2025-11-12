"""
``tests.unit.conftest`` module.

Configures pytest-based unit tests and provides shared fixtures.
"""
from __future__ import annotations

from typing import Any
from typing import Callable

import pytest


# SECTION: HELPERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: FIXTURES ========================================================= #


@pytest.fixture
def api_profile_defaults_factory() -> Callable[..., dict[str, Any]]:
    """
    Quick builder for profile defaults block dicts.

    Returns
    -------
    Callable[..., dict[str, Any]]
        Function that builds defaults mapping.

    Example
    -------
    defaults = api_profile_defaults_factory(
        pagination={'type': 'page', 'page_param': 'p', 'size_param': 's'},
        rate_limit={'sleep_seconds': 0.1, 'max_per_sec': 5},
        headers={'X': '1'},
    )
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
