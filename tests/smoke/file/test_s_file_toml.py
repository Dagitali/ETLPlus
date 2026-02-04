"""
:mod:`tests.smoke.file.test_s_file_toml` module.

Smoke tests for :mod:`etlplus.file.toml`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import toml as mod

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
        """
        path = tmp_path / 'data.toml'
        payload = sample_record

        try:
            written = mod.write(path, payload)
            assert written
            result = mod.read(path)
            assert result
        except ImportError as e:
            pytest.skip(str(e))
