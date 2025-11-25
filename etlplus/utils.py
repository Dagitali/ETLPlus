"""
etlplus.utils module.

Small shared helpers used across modules.
"""
from __future__ import annotations

import argparse
import json
from typing import Any

from .types import JSONData


# SECTION: EXPORTS ========================================================== #


__all__ = [
    'count_records',
    'json_type',
    'print_json',
    'to_float',
    'to_int',
    'to_number',
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


def json_type(option: str) -> Any:
    """
    Argparse ``type=`` hook that parses a JSON string.

    Parameters
    ----------
    option : str
        Raw CLI string to parse as JSON.

    Returns
    -------
    Any
        Parsed JSON value.

    Raises
    ------
    argparse.ArgumentTypeError
        If the input cannot be parsed as JSON.
    """
    try:
        return json.loads(option)
    except json.JSONDecodeError as e:  # pragma: no cover - argparse path
        raise argparse.ArgumentTypeError(
            f'Invalid JSON: {e.msg} (pos {e.pos})',
        ) from e


def print_json(obj: Any) -> None:
    """
    Pretty-print JSON to stdout using UTF-8 without ASCII escaping.

    Parameters
    ----------
    obj : Any
        Object to serialize as JSON.
    """
    print(json.dumps(obj, indent=2, ensure_ascii=False))


def to_float(value: Any) -> float | None:
    """
    Coerce a value to a float or return ``None``.

    Notes
    -----
    For strings, leading/trailing whitespace is ignored. Returns ``None``
    when coercion fails or when the input is ``None``.
    """
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        if isinstance(value, str):
            s = value.strip()
            try:
                return float(s)
            except ValueError:
                return None
        return None


def to_int(value: Any) -> int | None:
    """
    Coerce a value to an integer or return ``None``.

    Notes
    -----
    For strings, leading/trailing whitespace is ignored. Returns ``None``
    when coercion fails or when the input is ``None``.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        # Attempt float(string) -> int if string contains decimal .0
        if isinstance(value, str):
            s = value.strip()
            try:
                f = float(s)
                # Only return int when it's an exact integer value
                return int(f) if f.is_integer() else None
            except ValueError:
                return None
        return None


def to_number(value: Any) -> float | None:
    """
    Coerce numeric string to number or return ``None``.

    Parameters
    ----------
    value : Any
        Value that may be an ``int``, ``float``, or string representing
        a number (leading/trailing whitespace is ignored).

    Returns
    -------
    float | None
        The coerced numeric value as a ``float`` when possible, else
        ``None`` if the input is not numeric.
    """
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        try:
            return float(s)
        except ValueError:
            return None
    return None
