"""
:mod:`etlplus.api.utils` module.

Small shared helpers for :mod:`etlplus.api` modules.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar


# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Float coercion
    'to_float',
    'to_maximum_float',
    'to_minimum_float',
    'to_positive_float',

    # Integer coercion
    'to_int',
    'to_maximum_int',
    'to_minimum_int',
    'to_positive_int',
]


# SECTION: TYPE ALIASES ===================================================== #


Num = TypeVar('Num', int, float)


# SECTION: PROTECTED FUNCTIONS ============================================== #


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


def _coerce_int(value: object) -> int | None:
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
    coercer : Callable[[object], NumberT | None]
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
    NumberT | None
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


# SECTION: FUNCTIONS ======================================================== #


def to_float(
    value: object,
    default: float | None = None,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float | None:
    """
    Coerce ``value`` to a float with optional fallback and bounds.

    Parameters
    ----------
    value : object
        Value to coerce.
    default : float | None, optional
        Fallback returned when coercion fails. Defaults to ``None``.
    minimum : float | None, optional
        Minimum allowed value.
    maximum : float | None, optional
        Maximum allowed value.

    Returns
    -------
    float | None
        Normalized float or ``default`` when coercion fails.
    """
    return _normalize_number(
        _coerce_float,
        value,
        default=default,
        minimum=minimum,
        maximum=maximum,
    )


def to_maximum_float(
    value: object,
    default: float,
) -> float:
    """
    Return maximum float value between a coerced ``value`` and ``default``.

    Parameters
    ----------
    value : object
        Value to coerce to float.
    default : float
        Fallback value if coercion fails.

    Returns
    -------
    float
       Maximum of the coerced float value and ``default``.
    """
    result = to_float(value, default)
    return max(_value_or_default(result, default), default)


def to_minimum_float(
    value: object,
    default: float,
) -> float:
    """
    Return minimum float value between a coerced ``value`` and ``default``.

    Parameters
    ----------
    value : object
        Value to coerce to float.
    default : float
        Fallback value if coercion fails.

    Returns
    -------
    float
        The minimum of the coerced float value and the default.
    """
    result = to_float(value, default)
    return min(_value_or_default(result, default), default)


def to_positive_float(
    value: object,
) -> float | None:
    """
    Coerce ``value`` to a float when strictly positive.

    Parameters
    ----------
    value : object
        Value to convert.

    Returns
    -------
    float | None
        Positive float if conversion succeeds and the value is greater than
        zero; ``None`` if not.
    """
    result = to_float(value)
    if result is None or result <= 0:
        return None
    return result


def to_int(
    value: object,
    default: int | None = None,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    """
    Coerce ``value`` to ``int`` with optional fallback and bounds.

    Parameters
    ----------
    value : object
        Value to coerce.
    default : int | None, optional
        Fallback returned when coercion fails. Defaults to ``None``.
    minimum : int | None, optional
        Minimum allowed value.
    maximum : int | None, optional
        Maximum allowed value.

    Returns
    -------
    int | None
        Normalized integer or ``default`` when coercion fails.
    """
    return _normalize_number(
        _coerce_int,
        value,
        default=default,
        minimum=minimum,
        maximum=maximum,
    )


def to_maximum_int(
    value: object,
    default: int,
) -> int:
    """
    Return the maximum integer value between a coerced ``value`` and
    ``default``.


    Parameters
    ----------
    value : object
        Value to coerce to integer.
    default : int
        Fallback value if coercion fails.

    Returns
    -------
    int
        The maximum of the coerced integer value and the default.
    """
    result = to_int(value, default)
    return max(_value_or_default(result, default), default)


def to_minimum_int(
    value: object,
    default: int,
) -> int:
    """
    Return the minimum integer value between a coerced ``value`` and
    ``default``.


    Parameters
    ----------
    value : object
        Value to coerce to integer.
    default : int
        Fallback value if coercion fails.

    Returns
    -------
    int
        The minimum of the coerced integer value and the default.
    """
    result = to_int(value, default)
    return min(_value_or_default(result, default), default)


def to_positive_int(
    value: object,
    default: int,
    *,
    minimum: int = 1,
) -> int:
    """
    Coerce ``value`` to a positive integer, enforcing a lower bound.

    Parameters
    ----------
    value : object
        Value to coerce to positive integer.
    default : int
        Fallback value if coercion fails.
    minimum : int, optional
        Minimum allowed value. Defaults to 1.

    Returns
    -------
    int
        Coerced integer value, at least ``minimum``.
    """
    result = to_int(value, default, minimum=minimum)
    return result if result is not None else minimum
