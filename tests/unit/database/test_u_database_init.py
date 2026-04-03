"""
:mod:`tests.unit.database.test_u_database_init` module.

Unit tests for :mod:`etlplus.database` package exports.
"""

from __future__ import annotations

import pytest

import etlplus.database as database_pkg
from etlplus.database._ddl import load_table_spec
from etlplus.database._ddl import render_table_sql
from etlplus.database._ddl import render_tables
from etlplus.database._ddl import render_tables_to_string
from etlplus.database._engine import engine
from etlplus.database._engine import load_database_url_from_config
from etlplus.database._engine import make_engine
from etlplus.database._engine import session
from etlplus.database._orm import Base
from etlplus.database._orm import build_models
from etlplus.database._orm import load_and_build_models
from etlplus.database._orm import resolve_type
from etlplus.database._schema import ColumnSpec
from etlplus.database._schema import ForeignKeySpec
from etlplus.database._schema import IdentitySpec
from etlplus.database._schema import IndexSpec
from etlplus.database._schema import PrimaryKeySpec
from etlplus.database._schema import TableSpec
from etlplus.database._schema import UniqueConstraintSpec
from etlplus.database._schema import load_table_specs

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


DATABASE_EXPORTS = [
    ('Base', Base),
    ('ColumnSpec', ColumnSpec),
    ('ForeignKeySpec', ForeignKeySpec),
    ('IdentitySpec', IdentitySpec),
    ('IndexSpec', IndexSpec),
    ('PrimaryKeySpec', PrimaryKeySpec),
    ('TableSpec', TableSpec),
    ('UniqueConstraintSpec', UniqueConstraintSpec),
    ('build_models', build_models),
    ('load_and_build_models', load_and_build_models),
    ('load_database_url_from_config', load_database_url_from_config),
    ('load_table_spec', load_table_spec),
    ('load_table_specs', load_table_specs),
    ('make_engine', make_engine),
    ('render_table_sql', render_table_sql),
    ('render_tables', render_tables),
    ('render_tables_to_string', render_tables_to_string),
    ('resolve_type', resolve_type),
    ('engine', engine),
    ('session', session),
]


# SECTION: TESTS ============================================================ #


class TestDatabasePackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert database_pkg.__all__ == [name for name, _value in DATABASE_EXPORTS]

    @pytest.mark.parametrize(('name', 'expected'), DATABASE_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(database_pkg, name) == expected
