"""
:mod:`tests.smoke.file.test_s_file_properties` module.

Smoke tests for :mod:`etlplus.file.properties`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import properties as mod

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
        """
        path = tmp_path / 'data.properties'
        payload = {k: str(v) for k, v in sample_record.items()}

        try:
            written = mod.write(path, payload)
            assert written
            result = mod.read(path)
            assert result
        except ImportError as e:
            pytest.skip(str(e))
