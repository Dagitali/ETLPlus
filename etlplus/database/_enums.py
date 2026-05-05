"""
:mod:`etlplus.database._enums` module.

Database-specific enums and helpers.
"""

from __future__ import annotations

from ..utils._enums import CoercibleStrEnum
from ..utils._types import StrStrMap

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Enums
    'DatabaseDialect',
    'ReferentialAction',
    'SqlTypeAffinity',
]


# SECTION: ENUMS ============================================================ #


class DatabaseDialect(CoercibleStrEnum):
    """Supported database dialect identifiers."""

    # -- Constants -- #

    DUCKDB = 'duckdb'
    MSSQL = 'mssql'
    MYSQL = 'mysql'
    ORACLE = 'oracle'
    POSTGRESQL = 'postgresql'
    SQLITE = 'sqlite'

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> StrStrMap:
        """
        Return a mapping of common aliases for each enum member.

        Returns
        -------
        StrStrMap
            A mapping of alias names to their corresponding enum member names.
        """
        return {
            'access': 'mssql',
            'azure-sql': 'mssql',
            'azuresql': 'mssql',
            'duck': 'duckdb',
            'mariadb': 'mysql',
            'ms sql': 'mssql',
            'ms-sql': 'mssql',
            'mssqlserver': 'mssql',
            'postgres': 'postgresql',
            'sql server': 'mssql',
            'sql-server': 'mssql',
            'sqlite3': 'sqlite',
        }


class ReferentialAction(CoercibleStrEnum):
    """Supported SQL referential actions for foreign key constraints."""

    # -- Constants -- #

    CASCADE = 'cascade'
    NO_ACTION = 'no action'
    RESTRICT = 'restrict'
    SET_DEFAULT = 'set default'
    SET_NULL = 'set null'

    # -- Getters -- #

    @property
    def sql(self) -> str:
        """Return the SQL clause spelling for this referential action."""
        return self.value.upper()

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> StrStrMap:
        """
        Return a mapping of common aliases for each enum member.

        Returns
        -------
        StrStrMap
            A mapping of alias names to their corresponding enum member names.
        """
        return {
            'no_action': 'no action',
            'noaction': 'no action',
            'set_default': 'set default',
            'setdefault': 'set default',
            'set_null': 'set null',
            'setnull': 'set null',
        }


class SqlTypeAffinity(CoercibleStrEnum):
    """
    Portable SQL type affinities used for inferred database schemas.

    Notes
    -----
    This enum intentionally models broad portable affinities, not every SQL
    type declaration. Concrete declarations such as ``NVARCHAR(100)`` and
    ``DECIMAL(12,2)`` should remain strings.
    """

    # -- Constants -- #

    BINARY = 'binary'
    BOOLEAN = 'boolean'
    DATE = 'date'
    DATETIME = 'datetime'
    INTEGER = 'integer'
    JSON = 'json'
    NUMERIC = 'numeric'
    REAL = 'real'
    TEXT = 'text'
    TIME = 'time'
    UUID = 'uuid'

    # -- Getters -- #

    @property
    def ddl_name(self) -> str:
        """Return a portable uppercase SQL type name."""
        if self is SqlTypeAffinity.BINARY:
            return 'BLOB'
        if self is SqlTypeAffinity.BOOLEAN:
            return 'BOOLEAN'
        return self.value.upper()

    # -- Class Methods -- #

    @classmethod
    def aliases(cls) -> StrStrMap:
        """
        Return a mapping of common aliases for each enum member.

        Returns
        -------
        StrStrMap
            A mapping of alias names to their corresponding enum member names.
        """
        return {
            'bigint': 'integer',
            'blob': 'binary',
            'bool': 'boolean',
            'bytes': 'binary',
            'char': 'text',
            'decimal': 'numeric',
            'double': 'real',
            'float': 'real',
            'int': 'integer',
            'number': 'numeric',
            'str': 'text',
            'string': 'text',
            'timestamp': 'datetime',
            'varchar': 'text',
        }
