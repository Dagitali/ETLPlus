"""
:mod:`tests.unit.file.test_u_file_log` module.

Unit tests for :mod:`etlplus.file.log`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import log as mod
from tests.unit.file._module_contracts import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


class TestLog:
    """Unit tests for :mod:`etlplus.file.log`."""

    def test_stub_contract(
        self,
        tmp_path: Path,
    ) -> None:
        """Test LOG stub handler module contract."""
        assert_stub_module_contract(
            mod,
            mod.LogFile,
            format_name='log',
            tmp_path=tmp_path,
        )
