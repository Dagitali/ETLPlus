"""
:mod:`tests.unit.file.test_u_file_pbf` module.

Unit tests for :mod:`etlplus.file.pbf`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pytest

from etlplus.file import pbf as mod
from etlplus.file.stub import StubFileHandlerABC
from tests.unit.file.conftest import assert_stub_module_operation_raises

# SECTION: TESTS ============================================================ #


class TestPbf:
    """Unit tests for :mod:`etlplus.file.pbf`."""

    @pytest.fixture
    def write_payload(self) -> list[dict[str, int]]:
        """Create a representative write payload."""
        return [{'id': 1}]

    def test_handler_inherits_stub_abc(self) -> None:
        """Test pbf handler class contract."""
        assert issubclass(mod.PbfFile, StubFileHandlerABC)
        assert mod.PbfFile.format.value == 'pbf'

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_operations_raise_not_implemented(
        self,
        tmp_path: Path,
        write_payload: list[dict[str, int]],
        operation: Literal['read', 'write'],
    ) -> None:
        """Test pbf module-level read/write placeholder behavior."""
        assert_stub_module_operation_raises(
            mod,
            format_name='pbf',
            operation=operation,
            path=tmp_path / 'data.pbf',
            write_payload=write_payload,
        )
