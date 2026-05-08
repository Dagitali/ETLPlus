"""
:mod:`etlplus.utils._numbers` module.

Numeric coercion utility helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from decimal import InvalidOperation
from math import isfinite
from typing import cast

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'FloatParser',
    'IntParser',
    # Functions
    'finite_decimal_or_none',
    'is_integer_value',
    'is_number_value',
]


# SECTION: INTERNAL TYPE ALIASES ============================================ #


type _Floatable = float | str | int


# SECTION: FUNCTIONS ======================================================== #


def finite_decimal_or_none(
    value: object,
) -> Decimal | None:
    """
    Return a finite :class:`Decimal` for numeric-like values.

    Parameters
    ----------
    value : object
        Value to coerce.

    Returns
    -------
    Decimal | None
        Finite :class:`Decimal` or ``None`` when coercion fails or value is
        non-finite.
    """
    if not isinstance(value, int | float | Decimal | str):
        return None
    try:
        decimal = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
    return decimal if decimal.is_finite() else None


def is_integer_value(
    value: object,
) -> bool:
    """
    Return whether *value* is an integer, excluding booleans.

    Parameters
    ----------
    value : object
        Value to test.

    Returns
    -------
    bool
        ``True`` when *value* is an integer but not a boolean.
    """
    return isinstance(value, int) and not isinstance(value, bool)


def is_number_value(
    value: object,
) -> bool:
    """
    Return whether *value* is an ``int`` or ``float``, excluding booleans.

    Parameters
    ----------
    value : object
        Value to test.

    Returns
    -------
    bool
        ``True`` when *value* is numeric but not a boolean.
    """
    return isinstance(value, int | float) and not isinstance(value, bool)


# SECTION: INTERNAL CLASSES ================================================= #


class _NumberParser:
    """Shared normalization helpers for numeric parser classes."""

    # -- Internal Class Methods -- #

    @classmethod
    def _bounded[Num: (int, float)](
        cls,
        parser: Callable[[object, Num | None], Num | None],
        value: object,
        default: Num,
        *,
        bound: Callable[[Num, Num], Num],
    ) -> Num:
        """Parse a value and compare it with a required default bound."""
        return bound(
            default if (result := parser(value, default)) is None else result, default,
        )

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
        minimum, maximum = cls._validate_bounds(minimum, maximum)
        result = coercer(value)
        if result is None:
            result = default
        if result is None:
            return None
        return cls._clamp(result, minimum, maximum)

    # -- Internal Static Methods -- #

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


# SECTION: CLASSES ========================================================== #


class FloatParser(_NumberParser):
    """Cohesive float-oriented parsing and normalization rules."""

    # -- Class Methods -- #

    @classmethod
    def at_least(
        cls,
        value: object,
        default: float,
    ) -> float:
        """
        Return the greater of *default* and the parsed float value.

        Parameters
        ----------
        value : object
            Value to coerce.
        default : float
            Baseline float value.

        Returns
        -------
        float
            Greater of *default* and parsed float value.
        """
        return cls._bounded(cls.parse, value, default, bound=max)

    @classmethod
    def at_most(
        cls,
        value: object,
        default: float,
    ) -> float:
        """
        Return the lesser of *default* and the parsed float value.

        Parameters
        ----------
        value : object
            Value to coerce.
        default : float
            Baseline float value.

        Returns
        -------
        float
            Lesser of *default* and parsed float value.
        """
        return cls._bounded(cls.parse, value, default, bound=min)

    @classmethod
    def coerce(
        cls,
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
            case float() if isfinite(value):
                return value
            case float():
                return None
            case int():
                return float(value)
            case str():
                text = value.strip()
                if not text:
                    return None
                try:
                    parsed = float(text)
                except ValueError:
                    return None
                return parsed if isfinite(parsed) else None
            case _:
                try:
                    parsed = float(cast(_Floatable, value))
                except (TypeError, ValueError):
                    return None
                return parsed if isfinite(parsed) else None

    @classmethod
    def parse(
        cls,
        value: object,
        default: float | None = None,
        minimum: float | None = None,
        maximum: float | None = None,
    ) -> float | None:
        """
        Coerce *value* to a float with optional fallback and bounds.

        Parameters
        ----------
        value : object
            Value to coerce.
        default : float | None, optional
            Fallback returned if coercion fails. Defaults to ``None``.
        minimum : float | None, optional
            Lower bound, inclusive. Defaults to ``None``.
        maximum : float | None, optional
            Upper bound, inclusive. Defaults to ``None``.

        Returns
        -------
        float | None
            Coerced and optionally clamped value.
        """
        return cls._normalize(
            cls.coerce,
            value,
            default=default,
            minimum=minimum,
            maximum=maximum,
        )

    @classmethod
    def positive(
        cls,
        value: object,
    ) -> float | None:
        """
        Return a positive float when coercion succeeds.

        Parameters
        ----------
        value : object
            Value to coerce.

        Returns
        -------
        float | None
            Positive float or ``None`` when coercion fails.
        """
        result = cls.parse(value)
        if result is None or result <= 0:
            return None
        return result


class IntParser(_NumberParser):
    """Cohesive integer-oriented parsing and normalization rules."""

    # -- Internal Static Methods -- #

    @staticmethod
    def _integral_from_float(
        candidate: float | None,
    ) -> int | None:
        """Return ``int(candidate)`` when *candidate* is integral."""
        if candidate is None or not candidate.is_integer():
            return None
        return int(candidate)

    # -- Class Methods -- #

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
        return cls._bounded(cls.parse, value, default, bound=max)

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
        return cls._bounded(cls.parse, value, default, bound=min)

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
                text = value.strip()
                if not text:
                    return None
                try:
                    return int(text)
                except ValueError:
                    return cls._integral_from_float(FloatParser.coerce(text))
            case _:
                return cls._integral_from_float(FloatParser.coerce(value))

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

        Parameters
        ----------
        value : object
            Value to coerce.
        default : int | None, optional
            Fallback returned if coercion fails. Defaults to ``None``.
        minimum : int | None, optional
            Lower bound, inclusive. Defaults to ``None``.
        maximum : int | None, optional
            Upper bound, inclusive. Defaults to ``None``.

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
        return (
            minimum
            if (result := cls.parse(value, default, minimum=minimum)) is None
            else result
        )
