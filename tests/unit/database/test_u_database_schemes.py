"""
:mod:`tests.unit.database.test_u_database_schemes` module.

Unit tests for :mod:`etlplus.database._schemes`.
"""

from __future__ import annotations

from collections.abc import Callable

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
        ('predicate', 'value', 'expected'),
        [
            (is_database_dsn, 'sqlite+pysqlite', True),
            (is_database_dsn, 'duckdb+duckdb_engine', True),
            (is_database_dsn, 'sqlite:///source.db', False),
            (is_database_dsn, 'payload.csv', False),
            (is_database_dsn, '', False),
            (is_database_dsn, 'https://example.com/data.json', False),
            (is_database_url, 'postgres://user@host/db', True),
            (is_database_url, 'postgresql+psycopg://user@host/db', True),
            (is_database_url, 'duckdb:///warehouse.duckdb', True),
            (is_database_url, 'duckdb+duckdb_engine:///warehouse.duckdb', True),
            (is_database_url, 'mysql+pymysql://user@host/db', True),
            (is_database_url, 'sqlite:///source.db', True),
            (is_database_url, 'sqlite+pysqlite:///:memory:', True),
            (is_database_url, 'mssql+pyodbc://user@host/db', True),
            (is_database_url, 'oracle+cx_oracle://user@host/db', True),
            (is_database_url, 'snowflake://user@account/db/schema', True),
            (is_database_url, 'bigquery://project/dataset', True),
            (is_database_url, '', False),
            (is_database_url, 'payload.csv', False),
            (is_database_url, 'https://example.com/data.json', False),
            (is_database_url, 'sqlite+pysqlite', False),
            (is_database_url, 'postgresql+://user@host/db', False),
            (is_database_url, 'postgresql+psycopg+extra://user@host/db', False),
        ],
        ids=lambda param: getattr(param, '__name__', None),
    )
    def test_database_scheme_predicates(
        self,
        predicate: Callable[[str], bool],
        value: str,
        expected: bool,
    ) -> None:
        """Test database DSN and URL detection."""
        assert predicate(value) is expected

    def test_database_schemes_are_generated_from_dialects(self) -> None:
        """Test database scheme constants come from dialect scheme metadata."""
        assert DATABASE_SCHEMES == tuple(
            prefix
            for dialect in DatabaseDialect
            for prefix in dialect.scheme_prefixes()
        )
