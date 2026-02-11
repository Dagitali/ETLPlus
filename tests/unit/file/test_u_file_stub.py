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

    def test_raise_not_implemented_supports_custom_format_name(self) -> None:
        """Test custom format names in placeholder error messages."""
        with pytest.raises(NotImplementedError, match='MAT read'):
            mod._raise_not_implemented('read', format_name='MAT')
        with pytest.raises(NotImplementedError, match='MAT write'):
            mod._raise_not_implemented('write', format_name='MAT')

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
