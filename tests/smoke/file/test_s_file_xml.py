"""
:mod:`tests.smoke.file.test_s_file_xml` module.

Smoke tests for :mod:`etlplus.file.xml`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import xml as mod

# SECTION: TESTS ============================================================ #


class TestXml:
    """
    Smoke tests for :mod:`etlplus.file.xml`.
    """

    def test_read_write(self, tmp_path: Path) -> None:
        """
        Test that :func:`read`/:func:`write` can be invoked with minimal
        payloads.
        """
        path = tmp_path / 'data.xml'
        payload = {'root': {'text': 'hello'}}

        try:
            written = mod.write(path, payload, root_tag='root')
            assert written
            result = mod.read(path)
            assert result
        except ImportError as e:
            pytest.skip(str(e))
