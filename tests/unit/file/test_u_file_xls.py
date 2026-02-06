"""
:mod:`tests.unit.file.test_u_file_xls` module.

Unit tests for :mod:`etlplus.file.xls`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import xls as mod

# SECTION: TESTS ============================================================ #


class TestXlsRead:
    """Unit tests for :func:`etlplus.file.xls.read`."""

    def test_read_wraps_import_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        make_import_error_reader: Callable[[str], object],
    ) -> None:
        """
        Test that :func:`read` raises an informative error when the required
        dependency is missing.
        """

        monkeypatch.setattr(
            mod,
            'get_pandas',
            lambda *_: make_import_error_reader('read_excel'),
        )

        with pytest.raises(ImportError, match='xlrd'):
            mod.read(tmp_path / 'data.xls')


class TestXlsWrite:
    """Unit tests for :func:`etlplus.file.xls.write`."""

    def test_write_not_supported(self, tmp_path: Path) -> None:
        """
        Test that :func:`write` raises an error indicating lack of support.
        """
        with pytest.raises(RuntimeError, match='read-only'):
            mod.write(tmp_path / 'data.xls', [{'id': 1}])
