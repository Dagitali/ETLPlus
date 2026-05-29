"""
:mod:`tests.unit.database.test_u_database_schemes` module.

Unit tests for :mod:`etlplus.database._schemes`.
"""

from __future__ import annotations

import pytest

from etlplus.database import DATABASE_SCHEMES
from etlplus.database import DatabaseDialect
from etlplus.database import is_database_dsn
from etlplus.database import is_database_url

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestDatabaseSchemes:
    """Unit tests for database scheme helpers."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        [
            pytest.param('sqlite+pysqlite', True, id='driver-dsn'),
            pytest.param('duckdb+duckdb_engine', True, id='duckdb-driver-dsn'),
            pytest.param('sqlite:///source.db', False, id='url-not-dsn'),
            pytest.param('payload.csv', False, id='file-path'),
        ],
    )
    def test_database_dsns_are_recognized(
        self,
        value: str,
        expected: bool,
    ) -> None:
        """Test database DSN detection stays distinct from URL detection."""
        assert is_database_dsn(value) is expected

    def test_database_schemes_are_generated_from_dialects(self) -> None:
        """Test database scheme constants come from dialect scheme metadata."""
        assert DATABASE_SCHEMES == tuple(
            prefix
            for dialect in DatabaseDialect
            for prefix in dialect.scheme_prefixes()
        )

    @pytest.mark.parametrize(
        'value',
        [
            pytest.param('postgres://user@host/db', id='postgres'),
            pytest.param('postgresql+psycopg://user@host/db', id='postgresql-driver'),
            pytest.param('duckdb:///warehouse.duckdb', id='duckdb'),
            pytest.param(
                'duckdb+duckdb_engine:///warehouse.duckdb',
                id='duckdb-driver',
            ),
            pytest.param('mysql+pymysql://user@host/db', id='mysql-driver'),
            pytest.param('sqlite:///source.db', id='sqlite'),
            pytest.param('sqlite+pysqlite:///:memory:', id='sqlite-driver'),
            pytest.param('mssql+pyodbc://user@host/db', id='mssql-driver'),
            pytest.param('oracle+cx_oracle://user@host/db', id='oracle-driver'),
            pytest.param('snowflake://user@account/db/schema', id='snowflake'),
            pytest.param('bigquery://project/dataset', id='bigquery'),
        ],
    )
    def test_database_urls_are_recognized(
        self,
        value: str,
    ) -> None:
        """Test known database URL schemes are recognized."""
        assert is_database_url(value) is True

    @pytest.mark.parametrize(
        'value',
        [
            pytest.param('', id='empty'),
            pytest.param('payload.csv', id='file-path'),
            pytest.param('https://example.com/data.json', id='api-url'),
        ],
    )
    def test_non_database_dsns_are_rejected(
        self,
        value: str,
    ) -> None:
        """Test non-database DSN values are not recognized."""
        assert is_database_dsn(value) is False

    @pytest.mark.parametrize(
        'value',
        [
            pytest.param('', id='empty'),
            pytest.param('payload.csv', id='file-path'),
            pytest.param('https://example.com/data.json', id='api-url'),
            pytest.param('sqlite+pysqlite', id='driver-name-without-url'),
        ],
    )
    def test_non_database_urls_are_rejected(
        self,
        value: str,
    ) -> None:
        """Test non-database URL values are not recognized."""
        assert is_database_url(value) is False
