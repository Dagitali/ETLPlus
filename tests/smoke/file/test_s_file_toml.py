"""
:mod:`tests.smoke.file.test_s_file_toml` module.

Smoke tests for :mod:`etlplus.file.toml`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import toml as mod
from tests.smoke.conftest import run_file_smoke

# SECTION: TESTS ============================================================ #


class TestToml:
    """
    Smoke tests for :mod:`etlplus.file.toml`.
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
        path = tmp_path / 'data.toml'
        payload = sample_record
        run_file_smoke(mod, path, payload)
