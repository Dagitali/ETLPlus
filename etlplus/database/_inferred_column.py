"""
:mod:`etlplus.database._inferred_column` module.

Inferred column specification suitable for portable database writes.
"""

from __future__ import annotations

from dataclasses import dataclass

from ._enums import SqlTypeAffinity

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'InferredColumn',
]


# SECTION: CLASSES ========================================================== #


@dataclass(frozen=True)
class InferredColumn:
    """
    Inferred column specification suitable for portable database writes.

    Attributes
    ----------
    name : str
        Unquoted column name.
    type_affinity : SqlTypeAffinity
        Portable SQL type affinity for this column.
    nullable : bool
        True if NULL values are allowed.
    """

    # -- Attributes -- #

    name: str
    type_affinity: SqlTypeAffinity
    nullable: bool = True

    # -- Properties -- #

    @property
    def odbc_type(self) -> str:
        """Return a portable SQL type name for ODBC-style DDL."""
        return self.type_affinity.ddl_name
