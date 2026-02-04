"""
:mod:`tests.smoke.file.test_s_file_sqlite` module.

Smoke tests for :mod:`etlplus.file.sqlite`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import sqlite as mod

# SECTION: TESTS ============================================================ #


class TestSqlite:
    """
    Smoke tests for :mod:`etlplus.file.sqlite`.
    """

    def test_read_write(
        self,
        tmp_path: Path,
        sample_records: list[dict[str, object]],
    ) -> None:
        """
        Test that :func:`read`/:func:`write` can be invoked with minimal
        payloads.
        """
        path = tmp_path / 'data.sqlite'
        payload = sample_records

        try:
            written = mod.write(path, payload)
            assert written
            result = mod.read(path)
            assert result
        except ImportError as e:
            pytest.skip(str(e))
