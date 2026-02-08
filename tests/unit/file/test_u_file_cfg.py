"""
:mod:`tests.unit.file.test_u_file_cfg` module.

Unit tests for :mod:`etlplus.file.cfg`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import cfg as mod
from tests.unit.file._module_contracts import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


class TestCfg:
    """Unit tests for :mod:`etlplus.file.cfg`."""

    def test_stub_contract(
        self,
        tmp_path: Path,
    ) -> None:
        """Test CFG stub handler module contract."""
        assert_stub_module_contract(
            mod,
            mod.CfgFile,
            format_name='cfg',
            tmp_path=tmp_path,
        )
