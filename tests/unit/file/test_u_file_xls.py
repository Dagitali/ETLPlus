"""
:mod:`tests.unit.file.test_u_file_xls` module.

Unit tests for :mod:`etlplus.file.xls`.
"""

from __future__ import annotations

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
    ) -> None:
        """
        Test that :func:`read` raises an informative error when the required
        dependency is missing.
        """
        class _FailPandas:
            """Stub pandas module that fails to import Excel reader."""

            def read_excel(
                self,
                path: Path,
                *,
                engine: str,
            ) -> object:  # noqa: ARG002
                """Simulate failure when reading an Excel file."""
                raise ImportError('missing')

        monkeypatch.setattr(mod, 'get_pandas', lambda *_: _FailPandas())

        with pytest.raises(ImportError, match='xlrd'):
            mod.read(tmp_path / 'data.xls')


class TestXlsWrite:
    """Unit tests for :func:`etlplus.file.xls.write`."""

    def test_write_is_not_supported(self, tmp_path: Path) -> None:
        """
        Test that :func:`write` raises an error indicating lack of support.
        """
        with pytest.raises(RuntimeError, match='XLS write is not supported'):
            mod.write(tmp_path / 'data.xls', [{'id': 1}])
