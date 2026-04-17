"""
:mod:`tests.unit.history.test_u_history_init` module.

Unit tests for :mod:`etlplus.history` package facade exports.
"""

from __future__ import annotations

import pytest

import etlplus.history as history_pkg
from etlplus.history._store import HISTORY_SCHEMA_VERSION
from etlplus.history._store import HistoryStore
from etlplus.history._store import JsonlHistoryStore
from etlplus.history._store import RunCompletion
from etlplus.history._store import RunRecord
from etlplus.history._store import RunState
from etlplus.history._store import SQLiteHistoryStore

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


# SECTION: HELPERS ========================================================== #

HISTORY_EXPORTS = [
    ('HistoryStore', HistoryStore),
    ('JsonlHistoryStore', JsonlHistoryStore),
    ('RunCompletion', RunCompletion),
    ('RunRecord', RunRecord),
    ('RunState', RunState),
    ('SQLiteHistoryStore', SQLiteHistoryStore),
    ('HISTORY_SCHEMA_VERSION', HISTORY_SCHEMA_VERSION),
]

# SECTION: TESTS ============================================================ #


class TestHistoryPackageExports:
    """Unit tests for package-level history exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert history_pkg.__all__ == [name for name, _value in HISTORY_EXPORTS]

    @pytest.mark.parametrize(('name', 'expected'), HISTORY_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(history_pkg, name) == expected
