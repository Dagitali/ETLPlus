"""
:mod:`tests.smoke.file.test_s_file_bson` module.

Smoke tests for :mod:`etlplus.file.bson`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import bson as mod

# SECTION: TESTS ============================================================ #


class TestBson:
    """
    Smoke tests for :mod:`etlplus.file.bson`.
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
        path = tmp_path / 'data.bson'
        payload = sample_records

        try:
            written = mod.write(path, payload)
            assert written
            result = mod.read(path)
            assert result
        except ImportError as e:
            pytest.skip(str(e))
