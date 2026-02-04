"""
:mod:`tests.smoke.file.test_s_file_txt` module.

Smoke tests for :mod:`etlplus.file.txt`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import txt as mod

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
        """
        path = tmp_path / 'data.txt'
        payload = {
            'text': '\n'.join(str(value) for value in sample_record.values()),
        }

        try:
            written = mod.write(path, payload)
            assert written
            result = mod.read(path)
            assert result
        except ImportError as e:
            pytest.skip(str(e))
