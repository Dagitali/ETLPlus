"""
:mod:`etlplus.api.utils` module.

Small shared helpers for :mod:`etlplus.api` modules.
"""
# SECTION: EXPORTS ========================================================== #


__all__ = [
    'to_float',
    'to_int',
    'to_positive_int',
]

# SECTION: FUNCTIONS ======================================================== #


def to_float(
    value: object,
    default: float | None = None,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float | None:
    """
    Coerce a value to a float, with optional fallback and bounds.

    Parameters
    ----------
    value : object
        Value to coerce to float.
    default : float | None, optional
        Fallback value if coercion fails. If None, returns None on failure.
    minimum : float | None, optional
        Lower bound for returned value. If set, result will not be less than
        this.
    maximum : float | None, optional
        Upper bound for returned value. If set, result will not be greater
        than this.

    Returns
    -------
    float | None
        Float value if coercion succeeds and within bounds, else `default` or
        `None`.

    Notes
    -----
    - Ignores leading/trailing whitespace for strings.
    - Returns `default` (or None) for bools, None, or failed coercion.
    - Applies bounds if specified.
    """
    match value:
        case None | bool():
            result = default
        case float():
            result = value
        case int():
            result = float(value)
        case str():
            try:
                result = float(value.strip())
            except ValueError:
                result = default
        case _:
            result = default

    if result is not None:
        if minimum is not None:
            result = max(result, minimum)
        if maximum is not None:
            result = min(result, maximum)
    return result


def to_int(
    value: object,
    default: int | None = None,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    """
    Coerce a value to an integer, with optional fallback and bounds.

    Parameters
    ----------
    value : object
        Value to coerce to integer.
    default : int | None, optional
        Fallback value if coercion fails. If None, returns None on failure.
    minimum : int | None, optional
        Lower bound for returned value. If set, result will not be less than
        this.
    maximum : int | None, optional
        Upper bound for returned value. If set, result will not be greater than
        this.

    Returns
    -------
    int | None
        Integer value if coercion succeeds and within bounds, else `default` or
        `None`.

    Notes
    -----
    - Ignores leading/trailing whitespace for strings.
    - Returns `default` (or None) for bools, None, or failed coercion.
    - Applies bounds if specified.
    - Floats must be exact integers (e.g., 2.0).
    """
    match value:
        case None | bool():
            result = default
        case int():
            result = value
        case float() if value.is_integer():
            result = int(value)
        case str():
            s = value.strip()
            try:
                result = int(s)
            except ValueError:
                f = to_float(s)
                result = (
                    int(f) if f is not None and f.is_integer() else default
                )
        case _:
            result = default

    if result is not None:
        if minimum is not None:
            result = max(result, minimum)
        if maximum is not None:
            result = min(result, maximum)
    return result


def to_positive_int(
    value: object,
    default: int,
    *,
    minimum: int = 1,
) -> int:
    """
    Coerce a value to a positive integer, enforcing a lower bound.

    Parameters
    ----------
    value : object
        Value to coerce to positive integer.
    default : int
        Fallback value if coercion fails.
    minimum : int, optional
        Lower bound for returned value. Defaults to 1.

    Returns
    -------
    int
        Integer greater than or equal to `minimum`.

    Notes
    -----
    - Returns `minimum` if result is less than `minimum`.
    - Uses :func:`to_int` for coercion.
    """
    result = to_int(value, default, minimum=minimum)
    return result if result is not None else minimum
