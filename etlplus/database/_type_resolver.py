"""
:mod:`etlplus.database._type_resolver` module.

Maps observed Python runtime types to portable SQL types.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from decimal import Decimal

from ._enums import SqlTypeAffinity

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'TypeResolver',
]


# SECTION: CLASSES ========================================================== #


_INTEGER_TYPES = frozenset({bool, int})
_REAL_TYPES = frozenset({float})
_NUMERIC_TYPES = frozenset({Decimal})
_BINARY_TYPES = frozenset({bytes, bytearray, memoryview})
_DATE_TYPES = frozenset({date})
_DATETIME_TYPES = frozenset({datetime})
_TIME_TYPES = frozenset({time})
_TEXT_TYPES = frozenset({str})
_NUMERIC_FAMILY_TYPES = _INTEGER_TYPES | _REAL_TYPES | _NUMERIC_TYPES
_SIMPLE_TYPE_RULES = (
    (_INTEGER_TYPES, SqlTypeAffinity.INTEGER),
    (_REAL_TYPES, SqlTypeAffinity.REAL),
    (_NUMERIC_TYPES, SqlTypeAffinity.NUMERIC),
    (_BINARY_TYPES, SqlTypeAffinity.BINARY),
    (_DATE_TYPES, SqlTypeAffinity.DATE),
    (_DATETIME_TYPES, SqlTypeAffinity.DATETIME),
    (_TIME_TYPES, SqlTypeAffinity.TIME),
    (_TEXT_TYPES, SqlTypeAffinity.TEXT),
)


@dataclass(frozen=True, slots=True)
class TypeResolver:
    """
    Maps observed Python runtime types to portable SQL types.

    Attributes
    ----------
    prefer_text_on_mixed : bool
        If True, TEXT is chosen for mixed-type columns.

    Methods
    -------
    resolve : SqlTypeAffinity
        Return a portable SQL type affinity.
    """

    # -- Attributes -- #

    prefer_text_on_mixed: bool = True

    # -- Instance Methods -- #

    def resolve(self, py_types: set[type]) -> SqlTypeAffinity:
        """
        Resolve observed Python types to a portable SQL type affinity.

        Parameters
        ----------
        py_types : set[type]
            Set of observed Python types in a column.

        Returns
        -------
        SqlTypeAffinity
            Portable SQL type affinity for the observed Python types.
        """
        if not py_types:
            return SqlTypeAffinity.TEXT

        observed_types = frozenset(py_types)

        for expected_types, affinity in _SIMPLE_TYPE_RULES:
            if observed_types <= expected_types:
                return affinity

        if observed_types <= _NUMERIC_FAMILY_TYPES:
            return (
                SqlTypeAffinity.REAL
                if observed_types <= (_INTEGER_TYPES | _REAL_TYPES)
                else SqlTypeAffinity.NUMERIC
            )

        # Arbitrary mix -> TEXT for portability
        return (
            SqlTypeAffinity.TEXT
            if self.prefer_text_on_mixed
            else SqlTypeAffinity.NUMERIC
        )
