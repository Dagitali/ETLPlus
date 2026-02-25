"""
:mod:`tests.unit.database.test_u_database_init` module.

Unit tests for :mod:`etlplus.database` package exports.
"""

from __future__ import annotations

import etlplus.database as database_pkg


class TestDatabasePackageExports:
    """Unit tests for top-level package exports."""

    def test_expected_symbols_are_exported(self) -> None:
        """Top-level package should expose documented API surface."""
        expected = {
            'Base',
            'build_models',
            'engine',
            'load_and_build_models',
            'load_database_url_from_config',
            'load_table_spec',
            'load_table_specs',
            'make_engine',
            'render_table_sql',
            'render_tables',
            'render_tables_to_string',
            'session',
        }
        assert expected.issubset(set(database_pkg.__all__))

    def test_exported_symbols_are_present(self) -> None:
        """Every exported name should resolve on the package module."""
        for name in database_pkg.__all__:
            assert hasattr(database_pkg, name)
