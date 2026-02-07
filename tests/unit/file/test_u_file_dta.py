"""
:mod:`tests.unit.file.test_u_file_dta` module.

Unit tests for :mod:`etlplus.file.dta`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import dta as mod
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from tests.unit.file.conftest import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: TESTS ============================================================ #


def test_dta_uses_single_dataset_scientific_abc() -> None:
    """Test dta handler base contract."""
    assert issubclass(mod.DtaFile, SingleDatasetScientificFileHandlerABC)
    assert mod.DtaFile.dataset_key == 'data'


def test_dta_rejects_non_default_dataset_key() -> None:
    """Test dta rejecting non-default dataset key overrides."""
    assert_single_dataset_rejects_non_default_key(
        mod.DtaFile(),
        suffix='dta',
    )


def test_dta_write_empty_payload_returns_zero(
    tmp_path: Path,
) -> None:
    """Test dta write returning zero for empty payloads."""
    path = tmp_path / 'data.dta'
    assert mod.write(path, []) == 0
    assert not path.exists()
