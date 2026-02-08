"""
:mod:`tests.unit.file.test_u_file_log` module.

Unit tests for :mod:`etlplus.file.log`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pytest

from etlplus.file import log as mod
from etlplus.file.stub import StubFileHandlerABC
from tests.unit.file.conftest import assert_stub_module_operation_raises

# SECTION: TESTS ============================================================ #


class TestLog:
    """Unit tests for :mod:`etlplus.file.log`."""

    @pytest.fixture
    def write_payload(self) -> list[dict[str, int]]:
        """Create a representative write payload."""
        return [{'id': 1}]

    def test_handler_inherits_stub_abc(self) -> None:
        """Test log handler class contract."""
        assert issubclass(mod.LogFile, StubFileHandlerABC)
        assert mod.LogFile.format.value == 'log'

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_operations_raise_not_implemented(
        self,
        tmp_path: Path,
        write_payload: list[dict[str, int]],
        operation: Literal['read', 'write'],
    ) -> None:
        """Test log module-level read/write placeholder behavior."""
        assert_stub_module_operation_raises(
            mod,
            format_name='log',
            operation=operation,
            path=tmp_path / 'data.log',
            write_payload=write_payload,
        )
