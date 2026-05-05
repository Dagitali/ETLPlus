"""
:mod:`tests.unit.database.test_u_database_enums` module.

Unit tests for :mod:`etlplus.database._enums` coercion helpers and behaviors.
"""

from __future__ import annotations

import pytest

from etlplus.database._enums import DatabaseDialect
from etlplus.database._enums import ReferentialAction
from etlplus.database._enums import SqlTypeAffinity

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
        ],
    )
    def test_coerce_aliases(
        self,
        value: str,
        expected: DatabaseDialect,
    ) -> None:
        """Test that dialect aliases coerce to the expected enum members."""
        assert DatabaseDialect.coerce(value) is expected


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
