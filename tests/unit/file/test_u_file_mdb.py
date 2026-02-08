"""
:mod:`tests.unit.file.test_u_file_mdb` module.

Unit tests for :mod:`etlplus.file.mdb`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import mdb as mod
from tests.unit.file._module_contracts import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


class TestMdb:
    """Unit tests for :mod:`etlplus.file.mdb`."""

    def test_stub_contract(
        self,
        tmp_path: Path,
    ) -> None:
        """Test MDB stub handler module contract."""
        assert_stub_module_contract(
            mod,
            mod.MdbFile,
            format_name='mdb',
            tmp_path=tmp_path,
        )
