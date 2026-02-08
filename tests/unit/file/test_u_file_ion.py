"""
:mod:`tests.unit.file.test_u_file_ion` module.

Unit tests for :mod:`etlplus.file.ion`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import ion as mod
from tests.unit.file._module_contracts import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


class TestIon:
    """Unit tests for :mod:`etlplus.file.ion`."""

    def test_stub_contract(
        self,
        tmp_path: Path,
    ) -> None:
        """Test ION stub handler module contract."""
        assert_stub_module_contract(
            mod,
            mod.IonFile,
            format_name='ion',
            tmp_path=tmp_path,
        )
