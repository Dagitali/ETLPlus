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

    # pylint: disable=protected-access

    def test_module_read_wrapper_emits_deprecation_warning(
        self,
        tmp_path: Path,
    ) -> None:
        """Test deprecated module read wrapper warning and failure mode."""
        path = tmp_path / 'data.stub'
        path.write_text('stub', encoding='utf-8')

        with pytest.warns(DeprecationWarning, match='is deprecated'):
            with pytest.raises(NotImplementedError, match='STUB read'):
                mod.read(path)

    def test_module_read_wrapper_honors_custom_format_name(
        self,
        tmp_path: Path,
    ) -> None:
        """Test deprecated read wrapper custom format override."""
        path = tmp_path / 'data.stub'
        path.write_text('stub', encoding='utf-8')

        with pytest.warns(DeprecationWarning, match='is deprecated'):
            with pytest.raises(NotImplementedError, match='MAT read'):
                mod.read(path, format_name='MAT')

    def test_module_write_wrapper_emits_deprecation_warning(
        self,
        tmp_path: Path,
    ) -> None:
        """Test deprecated module write wrapper warning and failure mode."""
        path = tmp_path / 'data.stub'

        with pytest.warns(DeprecationWarning, match='is deprecated'):
            with pytest.raises(NotImplementedError, match='STUB write'):
                mod.write(path, [{'id': 1}])

    def test_module_write_wrapper_honors_custom_format_name(
        self,
        tmp_path: Path,
    ) -> None:
        """Test deprecated write wrapper custom format override."""
        path = tmp_path / 'data.stub'

        with pytest.warns(DeprecationWarning, match='is deprecated'):
            with pytest.raises(NotImplementedError, match='MAT write'):
                mod.write(path, [{'id': 1}], format_name='MAT')

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

    def test_stub_path_uses_format_suffix(self) -> None:
        """Test helper path construction for stub handlers."""
        assert mod.StubFile()._stub_path() == Path('ignored.stub')

    def test_write_raises_not_implemented(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`write` always raises for stubbed formats."""
        with pytest.raises(NotImplementedError, match='STUB write'):
            mod.StubFile().write(tmp_path / 'data.stub', [{'id': 1}])
