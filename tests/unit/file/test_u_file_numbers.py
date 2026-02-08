"""
:mod:`tests.unit.file.test_u_file_numbers` module.

Unit tests for :mod:`etlplus.file.numbers`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import numbers as mod
from tests.unit.file._module_contracts import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


class TestNumbers:
    """Unit tests for :mod:`etlplus.file.numbers`."""

    def test_stub_contract(
        self,
        tmp_path: Path,
    ) -> None:
        """Test NUMBERS stub handler module contract."""
        assert_stub_module_contract(
            mod,
            mod.NumbersFile,
            format_name='numbers',
            tmp_path=tmp_path,
        )
