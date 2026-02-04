"""
:mod:`tests.smoke.file.test_s_file_properties` module.

Smoke tests for :mod:`etlplus.file.properties`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import properties as mod
from tests.smoke.conftest import run_file_smoke

# SECTION: TESTS ============================================================ #


class TestProperties:
    """
    Smoke tests for :mod:`etlplus.file.properties`.
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
        """
        path = tmp_path / 'data.properties'
        payload = {k: str(v) for k, v in sample_record.items()}
        run_file_smoke(mod, path, payload)
