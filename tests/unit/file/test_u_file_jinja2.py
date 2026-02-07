"""
:mod:`tests.unit.file.test_u_file_jinja2` module.

Unit tests for :mod:`etlplus.file.jinja2`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import jinja2 as mod
from tests.unit.file.conftest import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


def test_jinja2_stub_contract(
    tmp_path: Path,
) -> None:
    """Test jinja2 stub handler module contract."""
    assert_stub_module_contract(
        mod,
        mod.Jinja2File,
        format_name='jinja2',
        tmp_path=tmp_path,
    )
