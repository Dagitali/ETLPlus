"""
:mod:`tests.unit.file.test_u_file_vm` module.

Unit tests for :mod:`etlplus.file.vm`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import vm as mod
from tests.unit.file.conftest import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


def test_vm_stub_contract(
    tmp_path: Path,
) -> None:
    """Test vm stub handler module contract."""
    assert_stub_module_contract(
        mod,
        mod.VmFile,
        format_name='vm',
        tmp_path=tmp_path,
    )
