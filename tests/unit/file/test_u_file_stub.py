"""
:mod:`tests.unit.file.test_u_file_stub` module.

Unit tests for :mod:`etlplus.file.stub`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import stub as mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestStubReadWrite:
    """Unit tests for :mod:`etlplus.file.stub` handler behavior."""

    def test_module_has_no_legacy_read_write_wrappers(self) -> None:
        """Test removed module-level ``read``/``write`` wrapper symbols."""
        assert not hasattr(mod, 'read')
        assert not hasattr(mod, 'write')

    @pytest.mark.parametrize(
        ('operation', 'args'),
        [
            ('read', ()),
            ('write', ([{'id': 1}],)),
        ],
    )
    def test_operations_raise_not_implemented(
        self,
        tmp_path: Path,
        operation: str,
        args: tuple[object, ...],
    ) -> None:
        """Test that read/write operations always raise for stubbed formats."""
        with pytest.raises(NotImplementedError, match=f'STUB {operation}'):
            getattr(mod.StubFile(), operation)(tmp_path / 'data.stub', *args)

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_raise_not_implemented_supports_custom_format_name(
        self,
        operation: str,
    ) -> None:
        """Test custom format names in placeholder error messages."""
        with pytest.raises(NotImplementedError, match=f'MAT {operation}'):
            mod._raise_not_implemented(operation, format_name='MAT')

    def test_stub_path_uses_format_suffix(self) -> None:
        """Test helper path construction for stub handlers."""
        assert mod.StubFile()._stub_path() == Path('ignored.stub')
