"""
:mod:`tests.unit.history.test_u_history_init` module.

Unit tests for :mod:`etlplus.history` package facade exports.
"""

from __future__ import annotations

import pytest

import etlplus.history as history_pkg
import etlplus.history._store as store_mod

from ..pytest_export_contracts import assert_package_exports

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #

HISTORY_EXPORTS: tuple[tuple[str, object], ...] = (
    ('HistoryStore', store_mod.HistoryStore),
    ('JsonlHistoryStore', store_mod.JsonlHistoryStore),
    ('RunCompletion', store_mod.RunCompletion),
    ('RunRecord', store_mod.RunRecord),
    ('RunState', store_mod.RunState),
    ('SQLiteHistoryStore', store_mod.SQLiteHistoryStore),
    ('HISTORY_SCHEMA_VERSION', store_mod.HISTORY_SCHEMA_VERSION),
)

# SECTION: TESTS ============================================================ #


class TestHistoryPackageExports:
    """Unit tests for package-level history exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert_package_exports(
            package_module=history_pkg,
            expected_exports=HISTORY_EXPORTS,
        )

    @pytest.mark.parametrize(('name', 'expected'), HISTORY_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(history_pkg, name) == expected
