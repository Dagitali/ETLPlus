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
    # Functions
    'infer_database_dialect_and_driver',
]


# SECTION: ENUMS ============================================================ #


class DatabaseDialect(CoercibleStrEnum):
    """Supported database dialect identifiers."""

    # -- Constants -- #

    BIGQUERY = 'bigquery'
    DUCKDB = 'duckdb'
    MSSQL = 'mssql'
    MYSQL = 'mysql'
    ORACLE = 'oracle'
    POSTGRESQL = 'postgresql'
    SNOWFLAKE = 'snowflake'
    SQLITE = 'sqlite'

    # -- Getters -- #

    @property
    def uri_scheme(self) -> str:
        """
        Return the preferred URI scheme for this database dialect.

        Returns
        -------
        str
            The preferred URI scheme for this database dialect.
        """
        return self.value

    @property
    def uri_scheme_aliases(self) -> tuple[str, ...]:
        """
        Return accepted URI scheme aliases for this database dialect.

        Returns
        -------
        tuple[str, ...]
            Accepted URI scheme aliases for this database dialect.
        """
        if self is DatabaseDialect.POSTGRESQL:
            return ('postgres',)
        return ()

    # -- Instance Methods -- #

    def dsn_scheme(
        self,
        driver: str | None = None,
    ) -> str:
        """
        Return a SQLAlchemy-style dialect or dialect+driver DSN scheme.

        Parameters
        ----------
        driver : str | None, optional
            Optional driver name to append after ``+``.

        Returns
        -------
        str
            The SQLAlchemy-style dialect or dialect+driver DSN scheme.
        """
        if driver is None:
            return self.uri_scheme
        return f'{self.uri_scheme}+{driver}'

    def scheme_prefixes(self) -> tuple[str, ...]:
        """
        Return accepted URL and driver-DSN prefixes for this dialect.

        Returns
        -------
        tuple[str, ...]
            Accepted URL and driver-DSN prefixes for this dialect.
        """
        schemes = (*self.uri_scheme_aliases, self.uri_scheme)
        return tuple(
            prefix for scheme in schemes for prefix in (f'{scheme}://', f'{scheme}+')
        )

    def url_prefix(
        self,
        driver: str | None = None,
    ) -> str:
        """
        Return a database URL prefix for this dialect.

        Parameters
        ----------
        driver : str | None, optional
            Optional driver name to append after ``+``.

        Returns
        -------
        str
            The database URL prefix for this dialect.
        """
        return f'{self.dsn_scheme(driver)}://'

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
            'bq': 'bigquery',
            'duck': 'duckdb',
            'gcp-bigquery': 'bigquery',
            'google-bigquery': 'bigquery',
            'mariadb': 'mysql',
            'ms sql': 'mssql',
            'ms-sql': 'mssql',
            'mssqlserver': 'mssql',
            'postgres': 'postgresql',
            'sf': 'snowflake',
            'snowflake-db': 'snowflake',
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
        """
        Return the SQL clause spelling for this referential action.

        Returns
        -------
        str
            The SQL clause spelling for this referential action.
        """
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
        """
        Return a portable uppercase SQL type name.

        Returns
        -------
        str
            The portable uppercase SQL type name.
        """
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


# SECTION: INTERNAL CONSTANTS ============================================== #


_DATABASE_URI_SCHEME_DIALECTS: dict[str, DatabaseDialect] = {
    scheme: dialect
    for dialect in DatabaseDialect
    for scheme in (*dialect.uri_scheme_aliases, dialect.uri_scheme)
}


# SECTION: FUNCTIONS ======================================================== #


def infer_database_dialect_and_driver(
    value: object,
) -> tuple[DatabaseDialect | None, str | None]:
    """
    Infer a database dialect and optional driver from a dialect, URL, or DSN.

    Parameters
    ----------
    value : object
        A database dialect, URL, SQLAlchemy-style dialect+driver DSN, or
        dialect-like string.

    Returns
    -------
    tuple[DatabaseDialect | None, str | None]
        The inferred database dialect and optional driver name.
    """
    if isinstance(value, DatabaseDialect):
        return value, None

    text = str(value).strip()
    if not text:
        return None, None

    normalized = text.lower()
    scheme = normalized.split('://', maxsplit=1)[0]
    scheme, delimiter, driver = scheme.partition('+')

    if delimiter:
        if not driver:
            return None, None
        dialect = _DATABASE_URI_SCHEME_DIALECTS.get(scheme)
        return (dialect, driver) if dialect is not None else (None, None)
    if '://' in normalized:
        return _DATABASE_URI_SCHEME_DIALECTS.get(scheme), None
    return DatabaseDialect.try_coerce(normalized), None
