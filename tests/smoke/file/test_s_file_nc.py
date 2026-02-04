"""
:mod:`tests.smoke.file.test_s_file_nc` module.

Smoke tests for :mod:`etlplus.file.nc`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import nc as mod
from tests.smoke.conftest import run_file_smoke

# SECTION: TESTS ============================================================ #


class TestNc:
    """
    Smoke tests for :mod:`etlplus.file.nc`.
    """

    def test_read_write(
        self,
        tmp_path: Path,
        sample_records: list[dict[str, object]],
    ) -> None:
        """
        Test that :func:`read`/:func:`write` can be invoked with minimal
        payloads.

        Parameters
        ----------
        tmp_path : Path
            Pytest temporary directory.
        sample_records : list[dict[str, object]]
            Sample record payload.
        """
        path = tmp_path / 'data.nc'
        payload = sample_records
        run_file_smoke(mod, path, payload)
