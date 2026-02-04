"""
:mod:`tests.smoke.file.test_s_file_tab` module.

Smoke tests for :mod:`etlplus.file.tab`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import tab as mod
from tests.smoke.conftest import run_file_smoke

# SECTION: TESTS ============================================================ #


class TestTab:
    """
    Smoke tests for :mod:`etlplus.file.tab`.
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
        path = tmp_path / 'data.tab'
        payload = sample_records
        run_file_smoke(mod, path, payload)
