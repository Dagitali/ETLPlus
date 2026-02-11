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

    def test_module_level_helpers_keep_format_override_compatibility(
        self,
        tmp_path: Path,
    ) -> None:
        """Test deprecated wrappers keeping custom format-name errors."""
        path = tmp_path / 'data.stub'
        with pytest.deprecated_call(match='stub.read\\(\\) is deprecated'):
            with pytest.raises(NotImplementedError, match='MAT read'):
                mod.read(path, format_name='MAT')
        with pytest.deprecated_call(match='stub.write\\(\\) is deprecated'):
            with pytest.raises(NotImplementedError, match='MAT write'):
                mod.write(path, [{'id': 1}], format_name='MAT')

    def test_module_level_helpers_warn_and_raise_not_implemented(
        self,
        tmp_path: Path,
    ) -> None:
        """Test wrappers warning before raising NotImplementedError."""
        path = tmp_path / 'data.stub'
        with pytest.deprecated_call(match='stub.read\\(\\) is deprecated'):
            with pytest.raises(NotImplementedError, match='STUB read'):
                mod.read(path)
        with pytest.deprecated_call(match='stub.write\\(\\) is deprecated'):
            with pytest.raises(NotImplementedError, match='STUB write'):
                mod.write(path, [{'id': 1}])

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
