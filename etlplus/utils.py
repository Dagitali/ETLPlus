"""
ETLPlus Utilities
=================

Small shared helpers used across modules.
"""
from __future__ import annotations

from .types import JSONData


# SECTION: PUBLIC API ======================================================= #


__all__ = [
    'count_records',
]


# SECTION: FUNCTIONS ======================================================== #


def count_records(data: JSONData) -> int:
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
