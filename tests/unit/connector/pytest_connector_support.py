"""
:mod:`tests.unit.connector.pytest_connector_support` module.

Shared helpers for pytest-based unit tests of :mod:`etlplus.connector`.
"""

from __future__ import annotations

from collections.abc import Mapping


def assert_connector_fields(
    actual: object,
    expected: Mapping[str, object],
) -> None:
    """Assert that *actual* exposes the expected field values."""
    for field, value in expected.items():
        assert getattr(actual, field) == value
