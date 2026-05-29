"""
:mod:`tests.unit.database.test_u_database_enums` module.

Unit tests for :mod:`etlplus.database._enums` coercion helpers and behaviors.
"""

from __future__ import annotations

import pytest

from etlplus.database._enums import DatabaseDialect
from etlplus.database._enums import ReferentialAction
from etlplus.database._enums import SqlTypeAffinity
from etlplus.database._enums import infer_database_dialect_and_driver

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestDatabaseDialect:
    """Unit tests for :class:`etlplus.database._enums.DatabaseDialect`."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            ('Postgres', DatabaseDialect.POSTGRESQL),
            ('sqlite3', DatabaseDialect.SQLITE),
            ('sql server', DatabaseDialect.MSSQL),
            ('AZURE-SQL', DatabaseDialect.MSSQL),
            ('gcp-bigquery', DatabaseDialect.BIGQUERY),
            ('snowflake-db', DatabaseDialect.SNOWFLAKE),
        ],
    )
    def test_coerce_aliases(
        self,
        value: str,
        expected: DatabaseDialect,
    ) -> None:
        """Test that dialect aliases coerce to the expected enum members."""
        assert DatabaseDialect.coerce(value) is expected

    @pytest.mark.parametrize(
        ('dialect', 'expected'),
        [
            (DatabaseDialect.SQLITE, 'sqlite+pysqlite'),
            (DatabaseDialect.POSTGRESQL, 'postgresql+psycopg'),
        ],
    )
    def test_dsn_scheme_appends_driver(
        self,
        dialect: DatabaseDialect,
        expected: str,
    ) -> None:
        """Test that dialects generate SQLAlchemy driver DSN schemes."""
        assert dialect.dsn_scheme(expected.rsplit('+', maxsplit=1)[-1]) == expected

    def test_postgresql_scheme_prefixes_include_postgres_alias(self) -> None:
        """Test that PostgreSQL keeps its accepted URL scheme alias."""
        assert DatabaseDialect.POSTGRESQL.scheme_prefixes() == (
            'postgres://',
            'postgres+',
            'postgresql://',
            'postgresql+',
        )

    def test_url_prefix_appends_driver(self) -> None:
        """Test that dialects generate URL prefixes with optional drivers."""
        assert DatabaseDialect.MYSQL.url_prefix('pymysql') == 'mysql+pymysql://'

    def test_uri_scheme_returns_preferred_scheme(self) -> None:
        """Test that dialects expose their preferred URI scheme."""
        assert DatabaseDialect.POSTGRESQL.uri_scheme == 'postgresql'


class TestInferDatabaseDialectAndDriver:
    """Unit tests for database dialect and driver inference."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            (
                DatabaseDialect.SQLITE,
                (DatabaseDialect.SQLITE, None),
            ),
            (
                'Postgres',
                (DatabaseDialect.POSTGRESQL, None),
            ),
            (
                'postgres://user@host/db',
                (DatabaseDialect.POSTGRESQL, None),
            ),
            (
                'postgresql+psycopg://user@host/db',
                (DatabaseDialect.POSTGRESQL, 'psycopg'),
            ),
            (
                'sqlite+pysqlite',
                (DatabaseDialect.SQLITE, 'pysqlite'),
            ),
            (
                'postgresql+://user@host/db',
                (None, None),
            ),
            (
                'postgresql+psycopg+extra://user@host/db',
                (None, None),
            ),
            (
                'https://example.com/data.json',
                (None, None),
            ),
        ],
    )
    def test_infers_database_dialect_and_driver(
        self,
        value: object,
        expected: tuple[DatabaseDialect | None, str | None],
    ) -> None:
        """Test database dialect and driver inference from common inputs."""
        assert infer_database_dialect_and_driver(value) == expected


class TestReferentialAction:
    """Unit tests for :class:`etlplus.database._enums.ReferentialAction`."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            ('CASCADE', ReferentialAction.CASCADE),
            ('no_action', ReferentialAction.NO_ACTION),
            ('setnull', ReferentialAction.SET_NULL),
            ('set default', ReferentialAction.SET_DEFAULT),
        ],
    )
    def test_coerce_aliases(
        self,
        value: str,
        expected: ReferentialAction,
    ) -> None:
        """Test that referential action aliases coerce correctly."""
        assert ReferentialAction.coerce(value) is expected

    def test_sql_returns_uppercase_clause(self) -> None:
        """Test that ``sql`` returns the SQL spelling for an action."""
        assert ReferentialAction.SET_NULL.sql == 'SET NULL'


class TestSqlTypeAffinity:
    """Unit tests for :class:`etlplus.database._enums.SqlTypeAffinity`."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            ('INTEGER', SqlTypeAffinity.INTEGER),
            ('blob', SqlTypeAffinity.BINARY),
            ('decimal', SqlTypeAffinity.NUMERIC),
            ('varchar', SqlTypeAffinity.TEXT),
            ('timestamp', SqlTypeAffinity.DATETIME),
        ],
    )
    def test_coerce_aliases(
        self,
        value: str,
        expected: SqlTypeAffinity,
    ) -> None:
        """Test that type affinity aliases coerce correctly."""
        assert SqlTypeAffinity.coerce(value) is expected

    @pytest.mark.parametrize(
        ('affinity', 'expected'),
        [
            (SqlTypeAffinity.BINARY, 'BLOB'),
            (SqlTypeAffinity.BOOLEAN, 'BOOLEAN'),
            (SqlTypeAffinity.INTEGER, 'INTEGER'),
            (SqlTypeAffinity.TEXT, 'TEXT'),
        ],
    )
    def test_ddl_name(
        self,
        affinity: SqlTypeAffinity,
        expected: str,
    ) -> None:
        """Test portable uppercase SQL type spellings."""
        assert affinity.ddl_name == expected

    def test_invalid_value_raises_value_error(self) -> None:
        """Test that invalid values raise :class:`ValueError`."""
        with pytest.raises(ValueError, match='Invalid SqlTypeAffinity'):
            SqlTypeAffinity.coerce('geography')
