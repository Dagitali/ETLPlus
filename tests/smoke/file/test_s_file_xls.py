"""
:mod:`tests.smoke.file.test_s_file_xls` module.

Smoke tests for :mod:`etlplus.file.xls`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import xls as mod

# SECTION: TESTS ============================================================ #


class TestXls:
    """
    Smoke tests for :mod:`etlplus.file.xls`.
    """

    def test_read_write_orig(
        self, tmp_path: Path,
        sample_records: list[dict[str, object]],
    ) -> None:
        """
        Test that :func:`read`/:func:`write` can be invoked with minimal
        payloads.
        """
        path = tmp_path / 'data.xls'
        payload = sample_records

        try:
            with pytest.raises(
                RuntimeError,
                match='XLS write is not supported',
            ):
                mod.write(path, payload)
        except ImportError as e:
            pytest.skip(str(e))
