"""
:mod:`tests.smoke.file.test_s_file_pb` module.

Smoke tests for :mod:`etlplus.file.pb`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import pb as mod
from tests.smoke.conftest import run_file_smoke

# SECTION: TESTS ============================================================ #


class TestPb:
    """
    Smoke tests for :mod:`etlplus.file.pb`.
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
        path = tmp_path / 'data.pb'
        payload = {'payload_base64': 'aGVsbG8='}
        run_file_smoke(mod, path, payload)
