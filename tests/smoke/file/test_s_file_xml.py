"""
:mod:`tests.smoke.file.test_s_file_xml` module.

Smoke tests for :mod:`etlplus.file.xml`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import xml as mod
from tests.smoke.conftest import run_file_smoke

# SECTION: TESTS ============================================================ #


class TestXml:
    """
    Smoke tests for :mod:`etlplus.file.xml`.
    """

    def test_read_write(self, tmp_path: Path) -> None:
        """
        Test that :func:`read`/:func:`write` can be invoked with minimal
        payloads.

        Parameters
        ----------
        tmp_path : Path
            Pytest temporary directory.
        """
        path = tmp_path / 'data.xml'
        payload = {'root': {'text': 'hello'}}
        run_file_smoke(mod, path, payload, write_kwargs={'root_tag': 'root'})
