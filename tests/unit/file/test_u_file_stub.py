"""
:mod:`tests.unit.file.test_u_file_stub` module.

Unit tests for :mod:`etlplus.file.stub`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import stub as mod

# SECTION: TESTS ============================================================ #


class TestStubReadWrite:
    """Unit tests for :mod:`etlplus.file.stub` functions."""

    def test_read_raises_not_implemented(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`read` always raises for stubbed formats."""
        with pytest.raises(NotImplementedError, match='STUB read'):
            mod.StubFile().read(tmp_path / 'data.stub')

    def test_write_raises_not_implemented(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`write` always raises for stubbed formats."""
        with pytest.raises(NotImplementedError, match='STUB write'):
            mod.StubFile().write(tmp_path / 'data.stub', [{'id': 1}])

    def test_module_level_helpers_raise_not_implemented(
        self,
        tmp_path: Path,
    ) -> None:
        """Test module-level helper functions raising NotImplementedError."""
        path = tmp_path / 'data.stub'
        with pytest.raises(NotImplementedError, match='Stubbed read'):
            mod.read(path)
        with pytest.raises(NotImplementedError, match='Stubbed write'):
            mod.write(path, [{'id': 1}])
