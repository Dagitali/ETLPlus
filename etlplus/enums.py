"""
:mod:`etlplus.enums` module.

Shared enumeration base class.
"""

from __future__ import annotations

import enum
from typing import Self

from .types import StrStrMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Enums
    'CoercibleStrEnum',
]


# SECTION: CLASSES ========================================================== #


class CoercibleStrEnum(enum.StrEnum):
    """
    StrEnum with ergonomic helpers.

    Provides a DRY, class-level :meth:`coerce` that normalizes inputs and
    produces consistent, informative error messages. Also exposes
    :meth:`choices` for UI/validation and :meth:`try_coerce` for soft parsing.

    Notes
    -----
    - Values are normalized via ``str(value).strip().casefold()``.
    - If value matching fails, the raw string is tried as a member name.
    - Error messages enumerate allowed values for easier debugging.
    """

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> StrStrMap:
        """
        Return a mapping of common aliases for each enum member.

        Subclasses may override this method to provide custom aliases.

        Returns
        -------
        StrStrMap
            A mapping of alias strings to their corresponding enum member
            values or names.

        Notes
        -----
        - Alias keys are normalized via ``str(key).strip().casefold()``.
        - Alias values should be member values or member names.
        """
        return {}

    @classmethod
    def choices(cls) -> tuple[str, ...]:
        """
        Return the allowed string values for this enum.

        Returns
        -------
        tuple[str, ...]
            A tuple of allowed string values for this enum.
        """
        return tuple(member.value for member in cls)

    @classmethod
    def coerce(cls, value: Self | str | object) -> Self:
        """
        Convert an enum member or string-like input to a member of *cls*.

        Parameters
        ----------
        value : Self | str | object
            An existing enum member or a string-like value to normalize.

        Returns
        -------
        Self
            The corresponding enum member.

        Raises
        ------
        ValueError
            If the value cannot be coerced into a valid member.
        """
        if isinstance(value, cls):
            return value
        try:
            raw = str(value).strip()
            normalized = raw.casefold()
            aliases = {
                str(key).strip().casefold(): alias
                for key, alias in cls.aliases().items()
            }
            resolved = aliases.get(normalized)
            if resolved is None:
                try:
                    return cls(normalized)  # type: ignore[arg-type]
                except (ValueError, TypeError):
                    return cls[raw]  # type: ignore[index]
            if isinstance(resolved, cls):
                return resolved
            try:
                return cls(resolved)  # type: ignore[arg-type]
            except (ValueError, TypeError):
                # Allow aliases to reference member names.
                return cls[resolved]  # type: ignore[index]
        except (ValueError, TypeError, KeyError) as e:
            allowed = ', '.join(cls.choices())
            raise ValueError(
                f'Invalid {cls.__name__} value: {value!r}. Allowed: {allowed}',
            ) from e

    @classmethod
    def try_coerce(
        cls,
        value: Self | str | object,
    ) -> Self | None:
        """
        Attempt to coerce a value into the enum; return ``None`` on failure.

        Parameters
        ----------
        value : Self | str | object
            An existing enum member or a string-like value to normalize.

        Returns
        -------
        Self | None
            The corresponding enum member, or ``None`` if coercion fails.
        """
        try:
            return cls.coerce(value)
        except (ValueError, TypeError, KeyError):
            return None
