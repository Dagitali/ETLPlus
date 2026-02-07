"""
:mod:`tests.unit.file.test_u_file_mustache` module.

Unit tests for :mod:`etlplus.file.mustache`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import mustache as mod
from tests.unit.file.conftest import assert_stub_module_contract

# SECTION: TESTS ============================================================ #


def test_mustache_stub_contract(
    tmp_path: Path,
) -> None:
    """Test mustache stub handler module contract."""
    assert_stub_module_contract(
        mod,
        mod.MustacheFile,
        format_name='mustache',
        tmp_path=tmp_path,
    )
