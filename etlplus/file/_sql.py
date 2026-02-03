"""
:mod:`etlplus.file._sql` module.

Shared helpers for lightweight SQL-backed file formats.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from ..types import JSONList

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'DEFAULT_TABLE',
    'DUCKDB_DIALECT',
    'SQLITE_DIALECT',
    # Data Classes
    'SqlDialect',
    # Functions
    'collect_column_values',
    'coerce_sql_value',
    'infer_column_type',
    'quote_identifier',
    'resolve_table',
]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class SqlDialect:
    """
    Simple SQL type mapping for inferred column types.

    Attributes
    ----------
    text : str
        Textual column type name.
    integer : str
        Integer column type name.
    floating : str
        Floating-point column type name.
    boolean : str
        Boolean column type name.
    """

    text: str
    integer: str
    floating: str
    boolean: str


# SECTION: CONSTANTS ======================================================== #


DEFAULT_TABLE = 'data'

SQLITE_DIALECT = SqlDialect(
    text='TEXT',
    integer='INTEGER',
    floating='REAL',
    boolean='INTEGER',
)

DUCKDB_DIALECT = SqlDialect(
    text='VARCHAR',
    integer='BIGINT',
    floating='DOUBLE',
    boolean='BOOLEAN',
)


# SECTION: FUNCTIONS ======================================================== #


def coerce_sql_value(value: Any) -> Any:
    """
    Normalize values into SQL-compatible scalar types.

    Parameters
    ----------
    value : Any
        Value to normalize.

    Returns
    -------
    Any
        Scalar value or serialized JSON string for complex objects.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=True)


def collect_column_values(
    records: JSONList,
) -> tuple[list[str], dict[str, list[Any]]]:
    """
    Collect column names and values from record payloads.

    Parameters
    ----------
    records : JSONList
        Record payloads to scan.

    Returns
    -------
    tuple[list[str], dict[str, list[Any]]]
        Sorted column names and mapped column values.
    """
    columns = sorted({key for row in records for key in row})
    column_values: dict[str, list[Any]] = {col: [] for col in columns}
    for row in records:
        for column in columns:
            column_values[column].append(row.get(column))
    return columns, column_values


def infer_column_type(values: list[Any], dialect: SqlDialect) -> str:
    """
    Infer a SQL column type for the provided values.

    Parameters
    ----------
    values : list[Any]
        Sample values for a column.
    dialect : SqlDialect
        Dialect mapping for type names.

    Returns
    -------
    str
        Dialect-specific type name.
    """
    seen_bool = False
    seen_int = False
    seen_float = False
    seen_other = False
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            seen_bool = True
        elif isinstance(value, int):
            seen_int = True
        elif isinstance(value, float):
            seen_float = True
        else:
            seen_other = True
            break
    if seen_other:
        return dialect.text
    if seen_float:
        return dialect.floating
    if seen_int:
        return dialect.integer
    if seen_bool:
        return dialect.boolean
    return dialect.text


def quote_identifier(value: str) -> str:
    """
    Return a safely quoted SQL identifier.

    Parameters
    ----------
    value : str
        Identifier to quote.

    Returns
    -------
    str
        Quoted identifier.
    """
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def resolve_table(
    tables: list[str],
    *,
    engine_name: str,
    default_table: str = DEFAULT_TABLE,
) -> str | None:
    """
    Pick a table name for read operations.

    Parameters
    ----------
    tables : list[str]
        Table names available in the database.
    engine_name : str
        Engine name used in error messages.
    default_table : str, optional
        Preferred table name to look for first.

    Returns
    -------
    str | None
        Selected table name or ``None`` when no tables exist.

    Raises
    ------
    ValueError
        If multiple candidate tables exist.
    """
    if not tables:
        return None
    if default_table in tables:
        return default_table
    if len(tables) == 1:
        return tables[0]
    raise ValueError(
        f'Multiple tables found in {engine_name} file; expected '
        f'"{default_table}" or a single table',
    )
