"""
:mod:`etlplus.database._value_codec` module.

Normalizes Python values to DB-friendly representations.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from decimal import Decimal
from decimal import InvalidOperation
from typing import Any

from ._enums import SqlTypeAffinity

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'ValueCodec',
]


# SECTION: CLASSES ========================================================== #


@dataclass(slots=True)
class ValueCodec:
    """
    Normalizes Python values to DB-friendly representations.

    Attributes
    ----------
    keep_unknown_as_json : bool
        If True, complex values become JSON (TEXT). Else str(value).

    Methods
    -------
    to_db : Any
        Convert `value` into a representation compatible with `sql_type`.
    """

    # -- Attributes -- #

    keep_unknown_as_json: bool = True

    # -- Instance Methods -- #

    def to_db(
        self,
        value: Any,
        sql_type: SqlTypeAffinity | str,
    ) -> Any:
        """
        Convert `value` into a representation compatible with `sql_type`.

        Parameters
        ----------
        value : Any
            Arbitrary Python value (or None).
        sql_type : SqlTypeAffinity | str
            Portable type affinity or a string accepted by
            :class:`SqlTypeAffinity`.

        Returns
        -------
        Any
            Value normalized for DB insertion (or None).
        """
        if value is None:
            return None

        affinity = SqlTypeAffinity.coerce(sql_type)

        # Booleans -> 0/1
        if isinstance(value, bool):
            return int(value)

        # Branch on target SQL type first
        match affinity:
            case SqlTypeAffinity.INTEGER | SqlTypeAffinity.BOOLEAN:
                return self._to_int(value)
            case SqlTypeAffinity.REAL:
                return self._to_real(value)
            case SqlTypeAffinity.NUMERIC:
                return self._to_numeric_text(value)
            case SqlTypeAffinity.BINARY:
                return self._to_blob(value)
            case SqlTypeAffinity.JSON:
                return json.dumps(value, default=self._json_default)
            case _:
                return self._to_text(value)

    # -- Internal Instance Methods -- #

    def _to_int(self, v: Any) -> int | None:
        value = self._decimal_or_none(v)
        return int(value) if value is not None else None

    def _to_real(self, v: Any) -> float | None:
        value = self._decimal_or_none(v)
        return float(value) if value is not None else None

    def _to_numeric_text(self, v: Any) -> str | None:
        if isinstance(v, str):
            return v
        value = self._decimal_or_none(v)
        if value is not None:
            return str(value)
        return None

    def _to_blob(self, v: Any) -> bytes:
        if isinstance(v, (bytes, bytearray, memoryview)):
            return bytes(v)
        enc = json.dumps(v, default=self._json_default)
        return enc.encode('utf-8')

    def _to_text(self, value: Any) -> str:
        """Return a portable text representation for a Python value."""
        match value:
            case datetime() | date() | time():
                return self._iso(value)
            case list() | dict() | tuple() | set() if self.keep_unknown_as_json:
                return json.dumps(value, default=self._json_default)
            case _:
                return str(value)

    # -- Internal Static Methods -- #

    @staticmethod
    def _decimal_or_none(value: Any) -> Decimal | None:
        """Return a finite :class:`Decimal` for numeric-like values."""
        if not isinstance(value, (int, float, Decimal, str)):
            return None
        try:
            decimal = (
                value if isinstance(value, Decimal) else Decimal(str(value).strip())
            )
        except (InvalidOperation, ValueError):
            return None
        return decimal if decimal.is_finite() else None

    @staticmethod
    def _iso(v: date | datetime | time) -> str:
        if isinstance(v, datetime):
            return v.isoformat(timespec='microseconds')
        if isinstance(v, time):
            return v.isoformat(timespec='microseconds')
        return v.isoformat()

    @staticmethod
    def _json_default(o: Any) -> Any:
        if isinstance(o, (date, datetime, time)):
            return ValueCodec._iso(o)
        if isinstance(o, Decimal):
            return str(o)
        return str(o)
