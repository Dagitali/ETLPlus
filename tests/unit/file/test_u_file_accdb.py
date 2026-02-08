"""
:mod:`tests.unit.file.test_u_file_accdb` module.

Unit tests for :mod:`etlplus.file.accdb`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import accdb as mod
from tests.unit.file._module_contracts import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


class TestAccdb:
    """Unit tests for :mod:`etlplus.file.accdb`."""

    def test_stub_contract(
        self,
        tmp_path: Path,
    ) -> None:
        """Test ACCDB stub handler module contract."""
        assert_stub_module_contract(
            mod,
            mod.AccdbFile,
            format_name='accdb',
            tmp_path=tmp_path,
        )
