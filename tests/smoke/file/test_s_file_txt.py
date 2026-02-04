"""
:mod:`tests.smoke.file.test_s_file_txt` module.

Smoke tests for :mod:`etlplus.file.txt`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import txt as mod
from tests.smoke.conftest import run_file_smoke

# SECTION: TESTS ============================================================ #


class TestTxt:
    """
    Smoke tests for :mod:`etlplus.file.txt`.
    """

    def test_read_write(
        self,
        tmp_path: Path,
        sample_record: dict[str, object],
    ) -> None:
        """
        Test that :func:`read`/:func:`write` can be invoked with minimal
        payloads.

        Parameters
        ----------
        tmp_path : Path
            Pytest temporary directory.
        sample_record : dict[str, object]
            Sample record payload.
        """
        path = tmp_path / 'data.txt'
        payload = {
            'text': '\n'.join(str(value) for value in sample_record.values()),
        }
        run_file_smoke(mod, path, payload)
