"""
:mod:`etlplus.database._type_resolver` module.

Maps observed Python runtime types to portable SQL types.
"""

from __future__ import annotations

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


_INTEGER_TYPES = {bool, int}
_REAL_TYPES = {float}
_NUMERIC_TYPES = {Decimal}
_BINARY_TYPES = {bytes, bytearray, memoryview}
_DATE_TYPES = {date}
_DATETIME_TYPES = {datetime}
_TIME_TYPES = {time}
_TEXT_TYPES = {str}
_NUMERIC_FAMILY_TYPES = _INTEGER_TYPES | _REAL_TYPES | _NUMERIC_TYPES


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

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(self, *, prefer_text_on_mixed: bool = True) -> None:
        self.prefer_text_on_mixed = prefer_text_on_mixed

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

        if py_types <= _INTEGER_TYPES:
            return SqlTypeAffinity.INTEGER

        if py_types <= _REAL_TYPES:
            return SqlTypeAffinity.REAL

        if py_types <= _NUMERIC_TYPES:
            return SqlTypeAffinity.NUMERIC

        if py_types <= _BINARY_TYPES:
            return SqlTypeAffinity.BINARY

        if py_types <= _DATE_TYPES:
            return SqlTypeAffinity.DATE

        if py_types <= _DATETIME_TYPES:
            return SqlTypeAffinity.DATETIME

        if py_types <= _TIME_TYPES:
            return SqlTypeAffinity.TIME

        if py_types <= _TEXT_TYPES:
            return SqlTypeAffinity.TEXT

        if py_types <= _NUMERIC_FAMILY_TYPES:
            return (
                SqlTypeAffinity.REAL
                if py_types <= (_INTEGER_TYPES | _REAL_TYPES)
                else SqlTypeAffinity.NUMERIC
            )

        # Arbitrary mix -> TEXT for portability
        return (
            SqlTypeAffinity.TEXT
            if self.prefer_text_on_mixed
            else SqlTypeAffinity.NUMERIC
        )
