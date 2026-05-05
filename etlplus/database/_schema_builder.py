"""
:mod:`etlplus.database._schema_builder` module.

Helpers for inferring schema from records.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ._enums import SqlTypeAffinity
from ._inferred_column import InferredColumn
from ._type_resolver import TypeResolver

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'SchemaBuilder',
]


# SECTION: TYPE ALIASES ===================================================== #


type TypeHints = Mapping[str, SqlTypeAffinity | str]


# SECTION: CLASSES ========================================================== #


class SchemaBuilder:
    """
    Infers columns with SQL types and nullability from records.

    Attributes
    ----------
    type_resolver : TypeResolver
        Strategy for mapping Python types to SQL type affinity.

    Methods
    -------
    infer_columns : list[InferredColumn]
        Return ordered inferred columns from the provided records.
    """

    # -- Magic Methods -- #

    def __init__(self, type_resolver: TypeResolver | None = None) -> None:
        self.type_resolver = type_resolver or TypeResolver()

    # -- Instance Methods -- #

    def infer_columns(
        self,
        recs: list[Mapping[str, Any]],
        *,
        type_hints: TypeHints | None = None,
    ) -> list[InferredColumn]:
        """
        Return ordered inferred columns from the provided records.

        Parameters
        ----------
        recs : list[Mapping[str, Any]]
            List of dict-like records to analyze.
        type_hints : TypeHints | None
            Optional mapping of column name to forced SQL type affinity.
        """
        names = sorted({k for r in recs for k in r.keys()})
        hints = _coerce_type_hints(type_hints)

        cols: list[InferredColumn] = []
        for name in names:
            if name in hints:
                nullable = any(r.get(name) is None for r in recs)
                cols.append(
                    InferredColumn(
                        name=name,
                        type_affinity=hints[name],
                        nullable=nullable,
                    ),
                )
                continue

            py_types: set[type] = set()
            has_none = False
            for r in recs:
                v = r.get(name, None)
                if v is None:
                    has_none = True
                    continue
                py_types.add(type(v))

            sql_type = self.type_resolver.resolve(py_types)
            cols.append(
                InferredColumn(
                    name=name,
                    type_affinity=sql_type,
                    nullable=has_none,
                ),
            )

        return cols


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_type_hints(
    type_hints: TypeHints | None,
) -> dict[str, SqlTypeAffinity]:
    """Normalize type hint values to SQL type affinities."""
    return {
        name: SqlTypeAffinity.coerce(value)
        for name, value in (type_hints or {}).items()
    }
