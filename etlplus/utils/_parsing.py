"""
:mod:`etlplus.utils._parsing` module.

Shared tolerant parsing helpers for configuration-style payloads.
"""

from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from typing import SupportsIndex
from typing import SupportsInt
from typing import TypeGuard

from ._text import TextNormalizer

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MappingFieldParser',
    'SequenceParser',
    'ValueParser',
]


# SECTION: CLASSES ========================================================== #


class ValueParser:
    """
    Centralize tolerant scalar parsing rules for config-like payloads.

    Notes
    -----
    - Optional string fields coerce non-``None`` values to strings.
    """

    # -- Static Methods -- #

    @staticmethod
    def bool_flag(
        value: object,
        *,
        default: bool,
        true_values: frozenset[str] = frozenset({'1', 'on', 'true', 'yes'}),
        false_values: frozenset[str] = frozenset({'0', 'off', 'false', 'no'}),
    ) -> bool:
        """
        Return one boolean flag parsed from common config/env values.

        Parameters
        ----------
        value : object
            Input value to parse.
        default : bool
            Fallback returned when *value* is not a recognized flag.
        true_values : frozenset[str], optional
            Normalized strings interpreted as ``True``.
        false_values : frozenset[str], optional
            Normalized strings interpreted as ``False``.

        Returns
        -------
        bool
            Parsed boolean or *default*.
        """
        if isinstance(value, bool):
            return value
        if not isinstance(value, str):
            return default
        normalized = TextNormalizer.normalize(value)
        if normalized in true_values:
            return True
        if normalized in false_values:
            return False
        return default

    @staticmethod
    def optional_choice(
        value: object,
        choices: Mapping[str, str],
    ) -> str | None:
        """
        Return one optional canonical choice string when recognized.

        Parameters
        ----------
        value : object
            Input value to parse as an optional choice.
        choices : Mapping[str, str]
            Mapping of normalized input choices to canonical outputs.

        Returns
        -------
        str | None
            Canonical choice when recognized, the stringified value when
            unrecognized, or ``None`` when *value* is missing.
        """
        if (text := ValueParser.optional_str(value)) is None:
            return None
        return choices.get(TextNormalizer.normalize(text), text)

    @staticmethod
    def optional_int(
        value: object,
        *,
        field_name: str,
        label: str,
    ) -> int | None:
        """
        Return an optional integer, rejecting booleans.

        Parameters
        ----------
        value : object
            Input value to parse as an optional integer.
        field_name : str
            Field name used in validation errors.
        label : str
            Human-readable payload label used in validation errors.

        Returns
        -------
        int | None
            Parsed integer value, or ``None`` when absent.

        Raises
        ------
        TypeError
            If the value cannot be parsed as an integer, or if it is a boolean.
        """
        if value is None:
            return None
        if isinstance(value, bool):
            raise TypeError(f'{label} "{field_name}" must be an integer')
        try:
            if not isinstance(
                value,
                str | bytes | bytearray | SupportsInt | SupportsIndex,
            ):
                raise TypeError
            return int(value)
        except (TypeError, ValueError) as exc:
            raise TypeError(f'{label} "{field_name}" must be an integer') from exc

    @staticmethod
    def optional_str(
        value: object,
    ) -> str | None:
        """
        Return an optional string, coercing non-``None`` values.

        Parameters
        ----------
        value : object
            Input value to parse as an optional string.

        Returns
        -------
        str | None
            The string value if present, otherwise None.
        """
        if value is None:
            return None
        return value if isinstance(value, str) else str(value)


class MappingFieldParser(ValueParser):
    """
    Parse required fields from mapping-style payloads.

    Notes
    -----
    - Mapping validation belongs in :mod:`etlplus.utils._mapping`.
    - This class only extracts typed fields once a mapping is already known.
    """

    # -- Static Methods -- #

    @staticmethod
    def required_str(
        data: Mapping[str, object],
        key: str,
    ) -> str | None:
        """
        Return one required string field from a mapping payload.

        Parameters
        ----------
        data : Mapping[str, object]
            Mapping payload to extract the field from.
        key : str
            Key of the required string field.

        Returns
        -------
        str | None
            The string value if present and valid, otherwise None.
        """
        return value if isinstance(value := data.get(key), str) else None

    @staticmethod
    def require_str(
        data: Mapping[str, object],
        key: str,
        *,
        label: str,
    ) -> str:
        """
        Return a required string field or raise a descriptive ``TypeError``.

        Parameters
        ----------
        data : Mapping[str, object]
            Mapping payload to extract the field from.
        key : str
            Key of the required string field.
        label : str
            Human-readable payload label used in the error message.

        Returns
        -------
        str
            The string value for *key*.

        Raises
        ------
        TypeError
            If *key* is missing or its value is not a string.
        """
        if (value := MappingFieldParser.required_str(data, key)) is not None:
            return value
        raise TypeError(f'{label} requires a "{key}" (str)')


class SequenceParser(ValueParser):
    """Parse sequence-style payloads into concrete sequence types."""

    # -- Static Methods -- #

    @staticmethod
    def is_non_text(
        value: object,
    ) -> TypeGuard[Sequence[object]]:
        """
        Return ``True`` for sequences excluding text and byte strings.

        Parameters
        ----------
        value : object
            Input value to test.

        Returns
        -------
        TypeGuard[Sequence[object]]
            ``True`` when *value* is a non-text sequence.
        """
        return isinstance(value, Sequence) and not isinstance(
            value,
            str | bytes | bytearray,
        )

    @staticmethod
    def str_list(
        value: object,
    ) -> list[str]:
        """
        Normalize a string or a sequence of strings into a string list.

        Parameters
        ----------
        value : object
            Input value to normalize.

        Returns
        -------
        list[str]
            Normalized list of strings.
        """
        if isinstance(value, str):
            return [value]
        if not SequenceParser.is_non_text(value):
            return []
        return [entry for entry in value if isinstance(entry, str)]
