"""
:mod:`tests.unit.file.test_u_file_sav` module.

Unit tests for :mod:`etlplus.file.sav`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import sav as mod
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from tests.unit.file.conftest import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: TESTS ============================================================ #


def test_sav_uses_single_dataset_scientific_abc() -> None:
    """Test sav handler base contract."""
    assert issubclass(mod.SavFile, SingleDatasetScientificFileHandlerABC)
    assert mod.SavFile.dataset_key == 'data'


def test_sav_rejects_non_default_dataset_key() -> None:
    """Test sav rejecting non-default dataset key overrides."""
    assert_single_dataset_rejects_non_default_key(
        mod.SavFile(),
        suffix='sav',
    )


def test_sav_write_empty_payload_returns_zero(
    tmp_path: Path,
) -> None:
    """Test sav write returning zero for empty payloads."""
    path = tmp_path / 'data.sav'
    assert mod.write(path, []) == 0
    assert not path.exists()
