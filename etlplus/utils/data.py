"""
:mod:`etlplus.utils.data` module.

Data-oriented utility helpers.
"""

from __future__ import annotations

import json
from typing import Any

from ..types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions (data utilities)
    'count_records',
    'print_json',
]


# SECTION: FUNCTIONS ======================================================== #


def count_records(
    data: JSONData,
) -> int:
    """
    Return a consistent record count for JSON-like data payloads.

    Lists are treated as multiple records; dicts as a single record.

    Parameters
    ----------
    data : JSONData
        Data payload to count records for.

    Returns
    -------
    int
        Number of records in `data`.
    """
    return len(data) if isinstance(data, list) else 1


def print_json(
    obj: Any,
) -> None:
    """
    Pretty-print *obj* as UTF-8 JSON without ASCII escaping.

    Parameters
    ----------
    obj : Any
        Object to serialize as JSON.

    Returns
    -------
    None
        This helper writes directly to STDOUT.
    """
    print(json.dumps(obj, indent=2, ensure_ascii=False))
