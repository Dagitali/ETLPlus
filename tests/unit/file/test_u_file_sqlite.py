"""
:mod:`tests.unit.file.test_u_file_sqlite` module.

Unit tests for :mod:`etlplus.file.sqlite`.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from etlplus.file import sqlite as mod

# SECTION: TESTS ============================================================ #


class TestSqliteRead:
    """Unit tests for :func:`etlplus.file.sqlite.read`."""

    def test_read_empty_database_returns_empty_list(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that reading an empty database returns an empty list."""
        path = tmp_path / 'empty.sqlite'
        sqlite3.connect(path).close()

        assert mod.read(path) == []

    def test_read_raises_on_multiple_tables(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that reading raises an error when multiple tables exist."""
        path = tmp_path / 'multi.sqlite'
        conn = sqlite3.connect(path)
        try:
            conn.execute('CREATE TABLE alpha (id INTEGER)')
            conn.execute('CREATE TABLE beta (id INTEGER)')
            conn.commit()
        finally:
            conn.close()

        with pytest.raises(
            ValueError,
            match='Multiple tables found in SQLite',
        ):
            mod.read(path)


class TestSqliteWrite:
    """Unit tests for :func:`etlplus.file.sqlite.write`."""

    def test_write_round_trip(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing and then reading returns the original data."""
        path = tmp_path / 'data.sqlite'
        payload = [{'id': 1, 'name': 'Ada'}, {'id': 2, 'name': 'Bob'}]

        written = mod.write(path, payload)

        assert written == 2
        assert mod.read(path) == payload

    def test_write_empty_payload_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing an empty payload returns zero."""
        path = tmp_path / 'data.sqlite'

        assert mod.write(path, []) == 0
