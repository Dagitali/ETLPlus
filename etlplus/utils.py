"""
:mod:`etlplus.utils` module.

Small shared helpers used across modules.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any
from typing import TypeVar

from .types import JSONData
from .types import StrAnyMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data utilities
    'count_records',
    'print_json',
    # Mapping utilities
    'cast_str_dict',
    'coerce_dict',
    'deep_substitute',
    'maybe_mapping',
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
    # Text processing
    'normalize_choice',
    'normalize_str',
]


# SECTION: TYPE VARS ======================================================== #


Num = TypeVar('Num', int, float)
# type Num = int | float


# SECTION: FUNCTIONS ======================================================== #


# -- Data Utilities -- #


def deep_substitute(
    value: Any,
    vars_map: StrAnyMap | None,
    env_map: Mapping[str, str] | None,
) -> Any:
    """
    Recursively substitute ``${VAR}`` tokens in nested structures.

    Only strings are substituted; other types are returned as-is.

    Parameters
    ----------
    value : Any
        The value to perform substitutions on.
    vars_map : StrAnyMap | None
        Mapping of variable names to replacement values (lower precedence).
    env_map : Mapping[str, str] | None
        Mapping of environment variables overriding *vars_map* values (higher
        precedence).

    Returns
    -------
    Any
        New structure with substitutions applied where tokens were found.
    """
    substitutions = _prepare_substitutions(vars_map, env_map)

    def _apply(node: Any) -> Any:
        match node:
            case str():
                return _replace_tokens(node, substitutions)
            case Mapping():
                return {k: _apply(v) for k, v in node.items()}
            case list() | tuple() as seq:
                apply = [_apply(item) for item in seq]
                return apply if isinstance(seq, list) else tuple(apply)
            case set():
                return {_apply(item) for item in node}
            case frozenset():
                return frozenset(_apply(item) for item in node)
            case _:
                return node

    return _apply(value)


def cast_str_dict(
    mapping: StrAnyMap | None,
) -> dict[str, str]:
    """
    Return a new ``dict`` with keys and values coerced to ``str``.

    Parameters
    ----------
    mapping : StrAnyMap | None
        Mapping to normalize; ``None`` yields ``{}``.

    Returns
    -------
    dict[str, str]
        Dictionary of the original key/value pairs converted via ``str()``.
    """
    if not mapping:
        return {}
    return {str(key): str(value) for key, value in mapping.items()}


def coerce_dict(
    value: Any,
) -> dict[str, Any]:
    """
    Return a ``dict`` copy when *value* is mapping-like.

    Parameters
    ----------
    value : Any
        Mapping-like object to copy. ``None`` returns an empty dict.

    Returns
    -------
    dict[str, Any]
        Shallow copy of *value* converted to a standard ``dict``.
    """
    return dict(value) if isinstance(value, Mapping) else {}


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


def maybe_mapping(
    value: Any,
) -> StrAnyMap | None:
    """
    Return *value* when it is mapping-like; otherwise ``None``.

    Parameters
    ----------
    value : Any
        Value to test.

    Returns
    -------
    StrAnyMap | None
        The input value if it is a mapping; ``None`` if not.
    """
    return value if isinstance(value, Mapping) else None


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


# -- Float Coercion -- #


def to_float(
    value: Any,
    default: float | None = None,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float | None:
    """
    Coerce *value* to a float with optional fallback and bounds.

    Notes
    -----
    For strings, leading/trailing whitespace is ignored. Returns ``None``
    when coercion fails and no *default* is provided.
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
    Return the greater of *default* and *value* after float coercion.

    Parameters
    ----------
    value : Any
        Candidate input coerced with :func:`to_float`.
    default : float
        Baseline float value that acts as the lower bound.

    Returns
    -------
    float
        *default* if coercion fails; else ``max(coerced, default)``.
    """
    result = to_float(value, default)
    return max(_value_or_default(result, default), default)


def to_minimum_float(
    value: Any,
    default: float,
) -> float:
    """
    Return the lesser of *default* and *value* after float coercion.

    Parameters
    ----------
    value : Any
        Candidate input coerced with :func:`to_float`.
    default : float
        Baseline float value that acts as the upper bound.

    Returns
    -------
    float
        *default* if coercion fails; else ``min(coerced, default)``.
    """
    result = to_float(value, default)
    return min(_value_or_default(result, default), default)


def to_positive_float(value: Any) -> float | None:
    """
    Return a positive float when coercion succeeds.

    Parameters
    ----------
    value : Any
        Value coerced using :func:`to_float`.

    Returns
    -------
    float | None
        Positive float if coercion succeeds and ``value > 0``; else ``None``.
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
    Coerce *value* to an integer with optional fallback and bounds.

    Notes
    -----
    For strings, leading/trailing whitespace is ignored. Returns ``None``
    when coercion fails and no *default* is provided.
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
    Return the greater of *default* and *value* after integer coercion.

    Parameters
    ----------
    value : Any
        Candidate input coerced with :func:`to_int`.
    default : int
        Baseline integer that acts as the lower bound.

    Returns
    -------
    int
        *default* if coercion fails; else ``max(coerced, default)``.
    """
    result = to_int(value, default)
    return max(_value_or_default(result, default), default)


def to_minimum_int(
    value: Any,
    default: int,
) -> int:
    """
    Return the lesser of *default* and *value* after integer coercion.

    Parameters
    ----------
    value : Any
        Candidate input coerced with :func:`to_int`.
    default : int
        Baseline integer acting as the upper bound.

    Returns
    -------
    int
        *default* if coercion fails; else ``min(coerced, default)``.
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
    Return a positive integer, falling back to *minimum* when needed.

    Parameters
    ----------
    value : Any
        Candidate input coerced with :func:`to_int`.
    default : int
        Fallback value when coercion fails; clamped by *minimum*.
    minimum : int
        Inclusive lower bound for the result. Defaults to ``1``.

    Returns
    -------
    int
        Positive integer respecting *minimum*.
    """
    result = to_int(value, default, minimum=minimum)
    return _value_or_default(result, minimum)


# -- Generic Number Coercion -- #


def to_number(
    value: object,
) -> float | None:
    """
    Coerce *value* to a ``float`` using the internal float coercer.

    Parameters
    ----------
    value : object
        Value that may be numeric or a numeric string. Booleans and blanks
        return ``None`` for consistency with :func:`to_float`.

    Returns
    -------
    float | None
        ``float(value)`` if coercion succeeds; else ``None``.
    """
    return _coerce_float(value)


# -- Text Processing -- #


def normalize_str(
    value: str | None,
) -> str:
    """
    Return lower-cased, trimmed text for normalization helpers.

    Parameters
    ----------
    value : str | None
        Optional user-provided text.

    Returns
    -------
    str
        Normalized string with surrounding whitespace removed and converted
        to lowercase. ``""`` when *value* is ``None``.
    """
    return (value or '').strip().lower()


def normalize_choice(
    value: str | None,
    *,
    mapping: Mapping[str, str],
    default: str,
    normalize: Callable[[str | None], str] = normalize_str,
) -> str:
    """
    Normalize a string choice using a mapping and fallback.

    Parameters
    ----------
    value : str | None
        Input value to normalize.
    mapping : Mapping[str, str]
        Mapping of acceptable normalized inputs to output values.
    default : str
        Default return value when input is missing or unrecognized.
    normalize : Callable[[str | None], str], optional
        Normalization function applied to *value*. Defaults to
        :func:`normalize_str`.

    Returns
    -------
    str
        Normalized mapped value or *default*.
    """
    return mapping.get(normalize(value), default)


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _clamp(
    value: Num,
    minimum: Num | None,
    maximum: Num | None,
) -> Num:
    """
    Return *value* constrained to the interval ``[minimum, maximum]``.

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


def _prepare_substitutions(
    vars_map: StrAnyMap | None,
    env_map: Mapping[str, Any] | None,
) -> tuple[tuple[str, Any], ...]:
    """
    Merge variable and environment maps into an ordered substitutions list.

    Parameters
    ----------
    vars_map : StrAnyMap | None
        Mapping of variable names to replacement values (lower precedence).
    env_map : Mapping[str, Any] | None
        Environment-backed values that override entries from *vars_map*.

    Returns
    -------
    tuple[tuple[str, Any], ...]
        Immutable sequence of ``(name, value)`` pairs suitable for token
        replacement.
    """
    if not vars_map and not env_map:
        return ()
    merged: dict[str, Any] = {**(vars_map or {}), **(env_map or {})}
    return tuple(merged.items())


def _replace_tokens(
    text: str,
    substitutions: Iterable[tuple[str, Any]],
) -> str:
    """
    Replace ``${VAR}`` tokens in *text* using *substitutions*.

    Parameters
    ----------
    text : str
        Input string that may contain ``${VAR}`` tokens.
    substitutions : Iterable[tuple[str, Any]]
        Sequence of ``(name, value)`` pairs used for token replacement.

    Returns
    -------
    str
        Updated text with replacements applied.
    """
    if not substitutions:
        return text
    out = text
    for name, replacement in substitutions:
        token = f'${{{name}}}'
        if token in out:
            out = out.replace(token, str(replacement))
    return out


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
    Return ``int(candidate)`` when *candidate* is integral.

    Parameters
    ----------
    candidate : float | None
        Float to convert when representing a whole number.

    Returns
    -------
    int | None
        Integer form of *candidate*; else ``None`` if not integral.
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
    Coerce *value* with *coercer* and optionally clamp it.

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
    Ensure *minimum* does not exceed *maximum*.

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
    if minimum is not None and maximum is not None and minimum > maximum:
        raise ValueError('minimum cannot exceed maximum')
    return minimum, maximum


def _value_or_default(
    value: Num | None,
    default: Num,
) -> Num:
    """
    Return *value* if not ``None``; else *default*.

    Parameters
    ----------
    value : Num | None
        Candidate value.
    default : Num
        Fallback value.

    Returns
    -------
    Num
        *value* or *default*.
    """
    return default if value is None else value
