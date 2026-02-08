"""
:mod:`tests.unit.file.test_u_file_vm` module.

Unit tests for :mod:`etlplus.file.vm`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pytest

from etlplus.file import vm as mod
from etlplus.file.stub import StubFileHandlerABC
from tests.unit.file.conftest import assert_stub_module_operation_raises

# SECTION: TESTS ============================================================ #


class TestVm:
    """Unit tests for :mod:`etlplus.file.vm`."""

    @pytest.fixture
    def write_payload(self) -> list[dict[str, int]]:
        """Create a representative write payload."""
        return [{'id': 1}]

    def test_handler_inherits_stub_abc(self) -> None:
        """Test vm handler class contract."""
        assert issubclass(mod.VmFile, StubFileHandlerABC)
        assert mod.VmFile.format.value == 'vm'

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_operations_raise_not_implemented(
        self,
        tmp_path: Path,
        write_payload: list[dict[str, int]],
        operation: Literal['read', 'write'],
    ) -> None:
        """Test vm module-level read/write placeholder behavior."""
        assert_stub_module_operation_raises(
            mod,
            format_name='vm',
            operation=operation,
            path=tmp_path / 'data.vm',
            write_payload=write_payload,
        )
