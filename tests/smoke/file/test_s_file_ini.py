"""
:mod:`tests.smoke.file.test_s_file_ini` module.

Smoke tests for :mod:`etlplus.file.ini`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import ini as mod

# SECTION: TESTS ============================================================ #


class TestIni:
    """
    Smoke tests for :mod:`etlplus.file.ini`.
    """

    def test_read_write(self, tmp_path: Path) -> None:
        """
        Test that :func:`read`/:func:`write` can be invoked with minimal
        payloads.
        """
        path = tmp_path / 'data.ini'
        payload = {'DEFAULT': {'name': 'Ada'}, 'main': {'age': '36'}}

        try:
            written = mod.write(path, payload)
            assert written
            result = mod.read(path)
            assert result
        except ImportError as e:
            pytest.skip(str(e))
