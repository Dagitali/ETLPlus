"""
:mod:`tests.unit.file.test_u_file_xpt` module.

Unit tests for :mod:`etlplus.file.xpt`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import xpt as mod
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from tests.unit.file.conftest import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: TESTS ============================================================ #


def test_xpt_uses_single_dataset_scientific_abc() -> None:
    """Test xpt handler base contract."""
    assert issubclass(mod.XptFile, SingleDatasetScientificFileHandlerABC)
    assert mod.XptFile.dataset_key == 'data'


def test_xpt_rejects_non_default_dataset_key() -> None:
    """Test xpt rejecting non-default dataset key overrides."""
    assert_single_dataset_rejects_non_default_key(
        mod.XptFile(),
        suffix='xpt',
    )


def test_xpt_write_empty_payload_returns_zero(
    tmp_path: Path,
) -> None:
    """Test xpt write returning zero for empty payloads."""
    path = tmp_path / 'data.xpt'
    assert mod.write(path, []) == 0
    assert not path.exists()
