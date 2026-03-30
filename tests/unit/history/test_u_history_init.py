"""
:mod:`tests.unit.history.test_u_history_init` module.

Unit tests for :mod:`etlplus.history` package exports.
"""

from __future__ import annotations

import etlplus.history as history_pkg
from etlplus.history._store import HISTORY_SCHEMA_VERSION

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestHistoryPackageExports:
    """Unit tests for package-level history exports."""

    def test_expected_symbols_are_exported(self) -> None:
        """Test that the history package exposes its stable public surface."""
        expected = {
            'HISTORY_SCHEMA_VERSION',
            'HistoryStore',
            'JsonlHistoryStore',
            'RunCompletion',
            'RunRecord',
            'RunState',
            'SQLiteHistoryStore',
            'build_run_record',
        }
        assert expected.issubset(set(history_pkg.__all__))

    def test_schema_version_constant_is_reexported(self) -> None:
        """Test that the package facade re-exports the schema constant."""
        assert history_pkg.HISTORY_SCHEMA_VERSION == HISTORY_SCHEMA_VERSION
