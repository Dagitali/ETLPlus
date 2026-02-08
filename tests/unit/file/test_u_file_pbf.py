"""
:mod:`tests.unit.file.test_u_file_pbf` module.

Unit tests for :mod:`etlplus.file.pbf`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import pbf as mod
from tests.unit.file._module_contracts import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


class TestPbf:
    """Unit tests for :mod:`etlplus.file.pbf`."""

    def test_stub_contract(
        self,
        tmp_path: Path,
    ) -> None:
        """Test PBF stub handler module contract."""
        assert_stub_module_contract(
            mod,
            mod.PbfFile,
            format_name='pbf',
            tmp_path=tmp_path,
        )
