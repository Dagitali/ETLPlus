"""
:mod:`tests.unit.file.test_u_file_conf` module.

Unit tests for :mod:`etlplus.file.conf`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import conf as mod
from tests.unit.file.conftest import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


def test_conf_stub_contract(
    tmp_path: Path,
) -> None:
    """Test conf stub handler module contract."""
    assert_stub_module_contract(
        mod,
        mod.ConfFile,
        format_name='conf',
        tmp_path=tmp_path,
    )
