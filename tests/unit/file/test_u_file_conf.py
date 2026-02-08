"""
:mod:`tests.unit.file.test_u_file_conf` module.

Unit tests for :mod:`etlplus.file.conf`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pytest

from etlplus.file import conf as mod
from etlplus.file.stub import StubFileHandlerABC
from tests.unit.file.conftest import assert_stub_module_operation_raises

# SECTION: TESTS ============================================================ #


class TestConf:
    """Unit tests for :mod:`etlplus.file.conf`."""

    @pytest.fixture
    def write_payload(self) -> list[dict[str, int]]:
        """Create a representative write payload."""
        return [{'id': 1}]

    def test_handler_inherits_stub_abc(self) -> None:
        """Test conf handler class contract."""
        assert issubclass(mod.ConfFile, StubFileHandlerABC)
        assert mod.ConfFile.format.value == 'conf'

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_operations_raise_not_implemented(
        self,
        tmp_path: Path,
        write_payload: list[dict[str, int]],
        operation: Literal['read', 'write'],
    ) -> None:
        """Test conf module-level read/write placeholder behavior."""
        assert_stub_module_operation_raises(
            mod,
            format_name='conf',
            operation=operation,
            path=tmp_path / 'data.conf',
            write_payload=write_payload,
        )
