"""
:mod:`tests.unit.file.test_u_file_mat` module.

Unit tests for :mod:`etlplus.file.mat`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import mat as mod
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from tests.unit.file.conftest import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: TESTS ============================================================ #


def test_mat_uses_single_dataset_scientific_abc() -> None:
    """Test mat handler base contract."""
    assert issubclass(mod.MatFile, SingleDatasetScientificFileHandlerABC)
    assert mod.MatFile.dataset_key == 'data'


def test_mat_rejects_non_default_dataset_key() -> None:
    """Test mat rejecting non-default dataset key overrides."""
    assert_single_dataset_rejects_non_default_key(
        mod.MatFile(),
        suffix='mat',
    )


def test_mat_read_write_raise_not_implemented(
    tmp_path: Path,
) -> None:
    """Test mat module-level read/write placeholder behavior."""
    path = tmp_path / 'data.mat'
    with pytest.raises(NotImplementedError, match='not implemented yet'):
        mod.read(path)
    with pytest.raises(NotImplementedError, match='not implemented yet'):
        mod.write(path, [{'id': 1}])
