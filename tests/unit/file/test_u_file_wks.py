"""
:mod:`tests.unit.file.test_u_file_wks` module.

Unit tests for :mod:`etlplus.file.wks`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import wks as mod
from tests.unit.file.conftest import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


def test_wks_stub_contract(
    tmp_path: Path,
) -> None:
    """Test wks stub handler module contract."""
    assert_stub_module_contract(
        mod,
        mod.WksFile,
        format_name='wks',
        tmp_path=tmp_path,
    )
