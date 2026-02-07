"""
:mod:`tests.unit.file.test_u_file_log` module.

Unit tests for :mod:`etlplus.file.log`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import log as mod
from tests.unit.file.conftest import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


def test_log_stub_contract(
    tmp_path: Path,
) -> None:
    """Test log stub handler module contract."""
    assert_stub_module_contract(
        mod,
        mod.LogFile,
        format_name='log',
        tmp_path=tmp_path,
    )
