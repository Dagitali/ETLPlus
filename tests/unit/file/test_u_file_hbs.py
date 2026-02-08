"""
:mod:`tests.unit.file.test_u_file_hbs` module.

Unit tests for :mod:`etlplus.file.hbs`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import hbs as mod
from tests.unit.file._module_contracts import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


class TestHbs:
    """Unit tests for :mod:`etlplus.file.hbs`."""

    def test_stub_contract(
        self,
        tmp_path: Path,
    ) -> None:
        """Test HBS stub handler module contract."""
        assert_stub_module_contract(
            mod,
            mod.HbsFile,
            format_name='hbs',
            tmp_path=tmp_path,
        )
