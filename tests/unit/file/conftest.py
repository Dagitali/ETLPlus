"""
:mod:`tests.unit.file.conftest` module.

Define shared fixtures and helpers for pytest-based unit tests of
:mod:`etlplus.file`.

Notes
-----
- Fixtures are designed for reuse and DRY test setup across file-focused unit
    tests.
"""

from __future__ import annotations

import math
import numbers
from collections.abc import Callable
from collections.abc import Generator

import pytest

import etlplus.file._imports as import_helpers
from etlplus.types import JSONData
from etlplus.types import JSONDict

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_numeric_value(
    value: object,
) -> object:
    """Coerce numeric scalars into stable Python numeric types."""
    if isinstance(value, numbers.Real):
        try:
            numeric = float(value)
            if math.isnan(numeric):
                return None
        except (TypeError, ValueError):
            return value
        if numeric.is_integer():
            return int(numeric)
        return float(numeric)
    return value


# SECTION: FUNCTIONS ======================================================== #


def normalize_numeric_records(
    records: JSONData,
) -> JSONData:
    """
    Normalize numeric record values for deterministic comparisons.

    Parameters
    ----------
    records : JSONData
        Record payloads to normalize.

    Returns
    -------
    JSONData
        Normalized record payloads.
    """
    if isinstance(records, list):
        normalized: list[JSONDict] = []
        for row in records:
            if not isinstance(row, dict):
                normalized.append(row)
                continue
            cleaned: JSONDict = {}
            for key, value in row.items():
                cleaned[key] = _coerce_numeric_value(value)
            normalized.append(cleaned)
        return normalized
    return records


def normalize_xml_payload(payload: JSONData) -> JSONData:
    """
    Normalize XML payloads to list-based item structures when possible.

    Parameters
    ----------
    payload : JSONData
        XML payload to normalize.

    Returns
    -------
    JSONData
        Normalized XML payload.
    """
    if not isinstance(payload, dict):
        return payload
    root = payload.get('root')
    if not isinstance(root, dict):
        return payload
    items = root.get('items')
    if isinstance(items, dict):
        root = {**root, 'items': [items]}
        return {**payload, 'root': root}
    return payload


def require_optional_modules(
    *modules: str,
) -> None:
    """
    Skip the test when optional dependencies are missing.

    Parameters
    ----------
    *modules : str
        Module names to verify via ``pytest.importorskip``.
    """
    for module in modules:
        pytest.importorskip(module)


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='optional_module_stub')
def optional_module_stub_fixture() -> Generator[
    Callable[[dict[str, object]], None]
]:
    """
    Install stub modules into the optional import cache.

    Clears the cache for deterministic tests, and restores it afterward.
    """
    cache = import_helpers._MODULE_CACHE  # pylint: disable=protected-access
    original = dict(cache)
    cache.clear()

    def _install(mapping: dict[str, object]) -> None:
        cache.update(mapping)

    try:
        yield _install
    finally:
        cache.clear()
        cache.update(original)
