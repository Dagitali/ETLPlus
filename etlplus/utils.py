"""
etlplus.utils module.

Small shared helpers used across modules.
"""
from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from typing import Any
from typing import TypeVar

from .types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data utilities
    'count_records',
    'json_type',
    'print_json',

    # Float coercion
    'to_float',
    'to_maximum_float',
    'to_minimum_float',
    'to_positive_float',

    # Int coercion
    'to_int',
    'to_maximum_int',
    'to_minimum_int',
    'to_positive_int',

    # Generic number coercion
    'to_number',
]


# SECTION: TYPE ALIASES ===================================================== #


Num = TypeVar('Num', int, float)


# SECTION: FUNCTIONS ======================================================== #


# -- Data Utilities -- #


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


def json_type(
    option: str,
) -> Any:
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


def print_json(
    obj: Any,
) -> None:
    """
    Pretty-print JSON to stdout using UTF-8 without ASCII escaping.

    Parameters
    ----------
    obj : Any
        Object to serialize as JSON.
    """
    print(json.dumps(obj, indent=2, ensure_ascii=False))


# -- Float Coercion -- #


def to_float(
    value: Any,
    default: float | None = None,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float | None:
    """
    Coerce ``value`` to a float with optional fallback and bounds.

    Notes
    -----
    For strings, leading/trailing whitespace is ignored. Returns ``None``
    when coercion fails and no ``default`` is provided.
    """
    return _normalize_number(
        _coerce_float,
        value,
        default=default,
        minimum=minimum,
        maximum=maximum,
    )


def to_maximum_float(
    value: Any,
    default: float,
) -> float:
    """
    Return the greater of ``default`` and the coerced float value.
    """
    result = to_float(value, default)
    return max(_value_or_default(result, default), default)


def to_minimum_float(
    value: Any,
    default: float,
) -> float:
    """
    Return the lesser of ``default`` and the coerced float value.
"""
    result = to_float(value, default)
    return min(_value_or_default(result, default), default)


def to_positive_float(value: Any) -> float | None:
    """
    Return a positive float when coercion succeeds.
    """
    result = to_float(value)
    if result is None or result <= 0:
        return None
    return result


# -- Int Coercion -- #


def to_int(
    value: Any,
    default: int | None = None,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    """
    Coerce ``value`` to an integer with optional fallback and bounds.

    Notes
    -----
    For strings, leading/trailing whitespace is ignored. Returns ``None``
    when coercion fails and no ``default`` is provided.
    """
    return _normalize_number(
        _coerce_int,
        value,
        default=default,
        minimum=minimum,
        maximum=maximum,
    )


def to_maximum_int(
    value: Any,
    default: int,
) -> int:
    """
    Return the greater of ``default`` and the coerced integer value.
    """
    result = to_int(value, default)
    return max(_value_or_default(result, default), default)


def to_minimum_int(
    value: Any,
    default: int,
) -> int:
    """
    Return the lesser of ``default`` and the coerced integer value.
    """
    result = to_int(value, default)
    return min(_value_or_default(result, default), default)


def to_positive_int(
    value: Any,
    default: int,
    *,
    minimum: int = 1,
) -> int:
    """
    Return a positive integer, falling back to ``minimum`` when needed.
    """
    result = to_int(value, default, minimum=minimum)
    return result if result is not None else minimum


# -- Generic Number Coercion -- #


def to_number(
    value: object,
) -> float | None:
    """
    Coerce numeric string to number or return ``None``.

    Parameters
    ----------
    value : object
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


# SECTION: PROTECTED FUNCTIONS ============================================= #


def _clamp(
    value: Num,
    minimum: Num | None,
    maximum: Num | None,
) -> Num:
    """
    Return ``value`` constrained to the interval ``[minimum, maximum]``.

    Parameters
    ----------
    value : Num
        Value to clamp.
    minimum : Num | None
        Minimum allowed value.
    maximum : Num | None
        Maximum allowed value.

    Returns
    -------
    Num
        Clamped value.
    """
    minimum, maximum = _validate_bounds(minimum, maximum)
    if minimum is not None:
        value = max(value, minimum)
    if maximum is not None:
        value = min(value, maximum)
    return value


def _coerce_float(
    value: object,
) -> float | None:
    """
    Best-effort float coercion that ignores booleans and blanks.

    Parameters
    ----------
    value : object
        Value to coerce.

    Returns
    -------
    float | None
        Coerced float or ``None`` when coercion fails.
    """
    match value:
        case None | bool():
            return None
        case float():
            return value
        case int():
            return float(value)
        case str():
            text = value.strip()
            if not text:
                return None
            try:
                return float(text)
            except ValueError:
                return None
        case _:
            try:
                return float(value)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return None


def _coerce_int(
    value: object,
) -> int | None:
    """
    Best-effort integer coercion allowing floats only when integral.

    Parameters
    ----------
    value : object
        Value to coerce.

    Returns
    -------
    int | None
        Coerced integer or ``None`` when coercion fails.
    """
    match value:
        case None | bool():
            return None
        case int():
            return value
        case float() if value.is_integer():
            return int(value)
        case str():
            text = value.strip()
            if not text:
                return None
            try:
                return int(text)
            except ValueError:
                return _integral_from_float(_coerce_float(text))
        case _:
            return _integral_from_float(_coerce_float(value))


def _integral_from_float(
    candidate: float | None,
) -> int | None:
    """
    Return ``int(candidate)`` when ``candidate`` is integral.

    Parameters
    ----------
    candidate : float | None
        Float to convert when representing a whole number.

    Returns
    -------
    int | None
        Integer form of ``candidate`` or ``None`` if not integral.
    """
    if candidate is None or not candidate.is_integer():
        return None
    return int(candidate)


def _normalize_number(
    coercer: Callable[[object], Num | None],
    value: object,
    *,
    default: Num | None = None,
    minimum: Num | None = None,
    maximum: Num | None = None,
) -> Num | None:
    """
    Coerce ``value`` with ``coercer`` and optionally clamp it.

    Parameters
    ----------
    coercer : Callable[[object], Num | None]
        Function that attempts coercion.
    value : object
        Value to normalize.
    default : Num | None, optional
        Fallback returned when coercion fails. Defaults to ``None``.
    minimum : Num | None, optional
        Lower bound, inclusive.
    maximum : Num | None, optional
        Upper bound, inclusive.

    Returns
    -------
    Num | None
        Normalized value or ``None`` when coercion fails.
    """
    result = coercer(value)
    if result is None:
        result = default
    if result is None:
        return None
    return _clamp(result, minimum, maximum)


def _validate_bounds(
    minimum: Num | None,
    maximum: Num | None,
) -> tuple[Num | None, Num | None]:
    """
    Ensure ``minimum`` does not exceed ``maximum``.

    Parameters
    ----------
    minimum : Num | None
        Candidate lower bound.
    maximum : Num | None
        Candidate upper bound.

    Returns
    -------
    tuple[Num | None, Num | None]
        Normalized ``(minimum, maximum)`` pair.

    Raises
    ------
    ValueError
        If both bounds are provided and ``minimum > maximum``.
    """
    if (
        minimum is not None
        and maximum is not None
        and minimum > maximum
    ):
        raise ValueError('minimum cannot exceed maximum')
    return minimum, maximum


def _value_or_default(
    value: Num | None,
    default: Num,
) -> Num:
    """
    Return ``value`` when not ``None``; otherwise ``default``.

    Parameters
    ----------
    value : Num | None
        Candidate value.
    default : Num
        Fallback value.

    Returns
    -------
    Num
        ``value`` or ``default``.
    """
    return default if value is None else value
