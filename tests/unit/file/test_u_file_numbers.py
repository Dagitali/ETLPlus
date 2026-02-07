"""
:mod:`tests.unit.file.test_u_file_numbers` module.

Unit tests for :mod:`etlplus.file.numbers`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import numbers as mod
from tests.unit.file.conftest import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


def test_numbers_stub_contract(
    tmp_path: Path,
) -> None:
    """Test numbers stub handler module contract."""
    assert_stub_module_contract(
        mod,
        mod.NumbersFile,
        format_name='numbers',
        tmp_path=tmp_path,
    )
