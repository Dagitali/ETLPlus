"""
:mod:`tests.unit.file.test_u_file_accdb` module.

Unit tests for :mod:`etlplus.file.accdb`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import accdb as mod
from tests.unit.file.conftest import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


def test_accdb_stub_contract(
    tmp_path: Path,
) -> None:
    """Test accdb stub handler module contract."""
    assert_stub_module_contract(
        mod,
        mod.AccdbFile,
        format_name='accdb',
        tmp_path=tmp_path,
    )
