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

import pytest

from etlplus.types import JSONData
from etlplus.types import JSONDict

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: HELPERS ========================================================== #


def require_optional_modules(*modules: str) -> None:
    """
    Skip the test when optional dependencies are missing.

    Parameters
    ----------
    *modules : str
        Module names to verify via ``pytest.importorskip``.
    """
    for module in modules:
        pytest.importorskip(module)


def _coerce_numeric_value(value: object) -> object:
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


def normalize_numeric_records(records: JSONData) -> JSONData:
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
