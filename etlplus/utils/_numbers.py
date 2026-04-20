"""
:mod:`etlplus.utils._numbers` module.

Numeric coercion utility helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions (float coercion)
    'to_float',
    'to_maximum_float',
    'to_minimum_float',
    'to_positive_float',
    # Functions (int coercion)
    'to_int',
    'to_maximum_int',
    'to_minimum_int',
    'to_positive_int',
    # Functions (generic number coercion)
    'to_number',
]


# SECTION: INTERNAL CLASSES ================================================= #


class _NumberParser:
    """Shared normalization helpers for numeric parser classes."""

    @staticmethod
    def _strip_text(
        value: str,
    ) -> str:
        """Return trimmed text used by numeric coercion helpers."""
        return value.strip()

    @classmethod
    def _clamp[Num: (int, float)](
        cls,
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
        minimum, maximum = cls._validate_bounds(minimum, maximum)
        if minimum is not None:
            value = max(value, minimum)
        if maximum is not None:
            value = min(value, maximum)
        return value

    @classmethod
    def _normalize[Num: (int, float)](
        cls,
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
            Function to coerce the value.
        value : object
            Value to coerce.
        default : Num | None, optional
            Fallback returned if coercion fails. Defaults to ``None``.
        minimum : Num | None, optional
            Lower bound, inclusive. Defaults to ``None``.
        maximum : Num | None, optional
            Upper bound, inclusive. Defaults to ``None``.

        Returns
        -------
        Num | None
            Coerced and optionally clamped value.
        """
        result = coercer(value)
        if result is None:
            result = default
        if result is None:
            return None
        return cls._clamp(result, minimum, maximum)

    @staticmethod
    def _validate_bounds[Num: (int, float)](
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

    @staticmethod
    def _value_or_default[Num: (int, float)](
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


class _FloatParser(_NumberParser):
    """Cohesive float-oriented parsing and normalization rules."""

    @classmethod
    def coerce(
        cls,
        value: object,
    ) -> float | None:
        """Best-effort float coercion that ignores booleans and blanks."""
        match value:
            case None | bool():
                return None
            case float():
                return value
            case int():
                return float(value)
            case str():
                text = cls._strip_text(value)
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

    @classmethod
    def parse(
        cls,
        value: object,
        default: float | None = None,
        minimum: float | None = None,
        maximum: float | None = None,
    ) -> float | None:
        """Coerce *value* to a float with optional fallback and bounds."""
        return cls._normalize(
            cls.coerce,
            value,
            default=default,
            minimum=minimum,
            maximum=maximum,
        )

    @classmethod
    def at_least(
        cls,
        value: object,
        default: float,
    ) -> float:
        """Return the greater of *default* and the parsed float value."""
        result = cls.parse(value, default)
        return max(cls._value_or_default(result, default), default)

    @classmethod
    def at_most(
        cls,
        value: object,
        default: float,
    ) -> float:
        """Return the lesser of *default* and the parsed float value."""
        result = cls.parse(value, default)
        return min(cls._value_or_default(result, default), default)

    @classmethod
    def positive(
        cls,
        value: object,
    ) -> float | None:
        """Return a positive float when coercion succeeds."""
        result = cls.parse(value)
        if result is None or result <= 0:
            return None
        return result


class _IntParser(_NumberParser):
    """Cohesive integer-oriented parsing and normalization rules."""

    @staticmethod
    def _integral_from_float(
        candidate: float | None,
    ) -> int | None:
        """Return ``int(candidate)`` when *candidate* is integral."""
        if candidate is None or not candidate.is_integer():
            return None
        return int(candidate)

    @classmethod
    def coerce(
        cls,
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
                text = cls._strip_text(value)
                if not text:
                    return None
                try:
                    return int(text)
                except ValueError:
                    return cls._integral_from_float(_FloatParser.coerce(text))
            case _:
                return cls._integral_from_float(_FloatParser.coerce(value))

    @classmethod
    def parse(
        cls,
        value: object,
        default: int | None = None,
        minimum: int | None = None,
        maximum: int | None = None,
    ) -> int | None:
        """
        Coerce *value* to an integer with optional fallback and bounds.

        Returns
        -------
        int | None
            Coerced integer or ``None`` when coercion fails.
        """
        return cls._normalize(
            cls.coerce,
            value,
            default=default,
            minimum=minimum,
            maximum=maximum,
        )

    @classmethod
    def at_least(
        cls,
        value: object,
        default: int,
    ) -> int:
        """
        Return the greater of *default* and the parsed integer value.

        Parameters
        ----------
        value : object
            Value to compare.
        default : int
            Baseline integer value.

        Returns
        -------
        int
            Greater of *default* and the parsed integer value.
        """
        result = cls.parse(value, default)
        return max(cls._value_or_default(result, default), default)

    @classmethod
    def at_most(
        cls,
        value: object,
        default: int,
    ) -> int:
        """
        Return the lesser of *default* and the parsed integer value.

        Parameters
        ----------
        value : object
            Value to compare.
        default : int
            Baseline integer value.

        Returns
        -------
        int
            Lesser of *default* and the parsed integer value.
        """
        result = cls.parse(value, default)
        return min(cls._value_or_default(result, default), default)

    @classmethod
    def positive(
        cls,
        value: object,
        default: int,
        *,
        minimum: int = 1,
    ) -> int:
        """
        Return a positive integer, falling back to *minimum* when needed.

        Parameters
        ----------
        value : object
            Value to coerce.
        default : int
            Baseline integer value.
        minimum : int, optional
            Minimum allowable value (default is 1).

        Returns
        -------
        int
            Positive integer if coercion succeeds; else *minimum*.
        """
        result = cls.parse(value, default, minimum=minimum)
        return cls._value_or_default(result, minimum)


# SECTION: FUNCTIONS ======================================================== #


# -- Float Coercion -- #


def to_float(
    value: object,
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
    return _FloatParser.parse(
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
    return _FloatParser.at_least(value, default)


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
    return _FloatParser.at_most(value, default)


def to_positive_float(
    value: Any,
) -> float | None:
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
    return _FloatParser.positive(value)


# -- Int Coercion -- #


def to_int(
    value: object,
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
    return _IntParser.parse(
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
    return _IntParser.at_least(value, default)


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
    return _IntParser.at_most(value, default)


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
    return _IntParser.positive(value, default, minimum=minimum)


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
    return _FloatParser.coerce(value)
