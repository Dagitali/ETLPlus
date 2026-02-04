"""
:mod:`tests.smoke.file.test_s_file_rda` module.

Smoke tests for :mod:`etlplus.file.rda`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import rda as mod
from tests.smoke.conftest import run_file_smoke

# SECTION: TESTS ============================================================ #


class TestRda:
    """
    Smoke tests for :mod:`etlplus.file.rda`.
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
        path = tmp_path / 'data.rda'
        payload = sample_records
        run_file_smoke(mod, path, payload)
