"""
:mod:`tests.smoke.file.test_s_file_ini` module.

Smoke tests for :mod:`etlplus.file.ini`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import ini as mod
from tests.smoke.conftest import run_file_smoke

# SECTION: TESTS ============================================================ #


class TestIni:
    """
    Smoke tests for :mod:`etlplus.file.ini`.
    """

    def test_read_write(self, tmp_path: Path) -> None:
        """
        Test that :func:`read`/:func:`write` can be invoked with minimal
        payloads.

        Parameters
        ----------
        tmp_path : Path
            Pytest temporary directory.
        """
        path = tmp_path / 'data.ini'
        payload = {'DEFAULT': {'name': 'Ada'}, 'main': {'age': '36'}}
        run_file_smoke(mod, path, payload)
