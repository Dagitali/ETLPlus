"""
:mod:`tests.smoke.file.test_s_file_pb` module.

Smoke tests for :mod:`etlplus.file.pb`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import pb as mod

# SECTION: TESTS ============================================================ #


class TestPb:
    """
    Smoke tests for :mod:`etlplus.file.pb`.
    """

    def test_read_write(self, tmp_path: Path) -> None:
        """
        Test that :func:`read`/:func:`write` can be invoked with minimal
        payloads.
        """
        path = tmp_path / 'data.pb'
        payload = {'payload_base64': 'aGVsbG8='}

        try:
            written = mod.write(path, payload)
            assert written
            result = mod.read(path)
            assert result
        except ImportError as e:
            pytest.skip(str(e))
