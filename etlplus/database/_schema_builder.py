"""
:mod:`etlplus.database._schema_builder` module.

Helpers for inferring schema from records.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
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


@dataclass(slots=True)
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

    # -- Attributes -- #

    type_resolver: TypeResolver = field(default_factory=TypeResolver)

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
        names = sorted({key for record in recs for key in record})
        hints = _coerce_type_hints(type_hints)

        columns: list[InferredColumn] = []
        for name in names:
            if name in hints:
                nullable = any(r.get(name) is None for r in recs)
                columns.append(
                    InferredColumn(
                        name=name,
                        type_affinity=hints[name],
                        nullable=nullable,
                    ),
                )
                continue

            py_types, nullable = _column_types(recs, name)
            columns.append(
                InferredColumn(
                    name=name,
                    type_affinity=self.type_resolver.resolve(py_types),
                    nullable=nullable,
                ),
            )

        return columns


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _coerce_type_hints(
    type_hints: TypeHints | None,
) -> dict[str, SqlTypeAffinity]:
    """Normalize type hint values to SQL type affinities."""
    return {
        name: SqlTypeAffinity.coerce(value)
        for name, value in (type_hints or {}).items()
    }


def _column_types(
    records: list[Mapping[str, Any]],
    name: str,
) -> tuple[set[type], bool]:
    """Return observed non-null Python types and nullability for a column."""
    values = [record.get(name) for record in records]
    return {type(value) for value in values if value is not None}, any(
        value is None for value in values
    )
