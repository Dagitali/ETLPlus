"""
:mod:`tests.unit.file.test_u_file_jinja2` module.

Unit tests for :mod:`etlplus.file.jinja2`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pytest

from etlplus.file import jinja2 as mod
from etlplus.file.stub import StubFileHandlerABC
from tests.unit.file.conftest import assert_stub_module_operation_raises

# SECTION: TESTS ============================================================ #


class TestJinja2:
    """Unit tests for :mod:`etlplus.file.jinja2`."""

    @pytest.fixture
    def write_payload(self) -> list[dict[str, int]]:
        """Create a representative write payload."""
        return [{'id': 1}]

    def test_handler_inherits_stub_abc(self) -> None:
        """Test jinja2 handler class contract."""
        assert issubclass(mod.Jinja2File, StubFileHandlerABC)
        assert mod.Jinja2File.format.value == 'jinja2'

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_operations_raise_not_implemented(
        self,
        tmp_path: Path,
        write_payload: list[dict[str, int]],
        operation: Literal['read', 'write'],
    ) -> None:
        """Test jinja2 module-level read/write placeholder behavior."""
        assert_stub_module_operation_raises(
            mod,
            format_name='jinja2',
            operation=operation,
            path=tmp_path / 'data.jinja2',
            write_payload=write_payload,
        )
