"""
:mod:`tests.unit.file.test_u_file_hbs` module.

Unit tests for :mod:`etlplus.file.hbs`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pytest

from etlplus.file import hbs as mod
from etlplus.file.stub import StubFileHandlerABC
from tests.unit.file.conftest import assert_stub_module_operation_raises

# SECTION: TESTS ============================================================ #


class TestHbs:
    """Unit tests for :mod:`etlplus.file.hbs`."""

    @pytest.fixture
    def write_payload(self) -> list[dict[str, int]]:
        """Create a representative write payload."""
        return [{'id': 1}]

    def test_handler_inherits_stub_abc(self) -> None:
        """Test hbs handler class contract."""
        assert issubclass(mod.HbsFile, StubFileHandlerABC)
        assert mod.HbsFile.format.value == 'hbs'

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_operations_raise_not_implemented(
        self,
        tmp_path: Path,
        write_payload: list[dict[str, int]],
        operation: Literal['read', 'write'],
    ) -> None:
        """Test hbs module-level read/write placeholder behavior."""
        assert_stub_module_operation_raises(
            mod,
            format_name='hbs',
            operation=operation,
            path=tmp_path / 'data.hbs',
            write_payload=write_payload,
        )
