"""
:mod:`tests.smoke.file.test_s_file_feather` module.

Smoke tests for :mod:`etlplus.file.feather`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import feather as mod

# SECTION: TESTS ============================================================ #


class TestFeather:
    """
    Smoke tests for :mod:`etlplus.file.feather`.
    """

    def test_read_write(
        self,
        tmp_path: Path,
        sample_records: list[dict[str, object]],
    ) -> None:
        """
        Test that :func:`read`/:func:`write` can be invoked with minimal
        payloads.
        """
        path = tmp_path / 'data.feather'
        payload = sample_records

        try:
            written = mod.write(path, payload)
            assert written
            result = mod.read(path)
            assert result
        except ImportError as e:
            pytest.skip(str(e))
