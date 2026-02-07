"""
:mod:`tests.unit.file.test_u_file_sylk` module.

Unit tests for :mod:`etlplus.file.sylk`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import sylk as mod
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from tests.unit.file.conftest import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: TESTS ============================================================ #


def test_sylk_uses_single_dataset_scientific_abc() -> None:
    """Test sylk handler base contract."""
    assert issubclass(mod.SylkFile, SingleDatasetScientificFileHandlerABC)
    assert mod.SylkFile.dataset_key == 'data'


def test_sylk_rejects_non_default_dataset_key() -> None:
    """Test sylk rejecting non-default dataset key overrides."""
    assert_single_dataset_rejects_non_default_key(
        mod.SylkFile(),
        suffix='sylk',
    )


def test_sylk_read_write_raise_not_implemented(
    tmp_path: Path,
) -> None:
    """Test sylk module-level read/write placeholder behavior."""
    path = tmp_path / 'data.sylk'
    with pytest.raises(NotImplementedError, match='not implemented yet'):
        mod.read(path)
    with pytest.raises(NotImplementedError, match='not implemented yet'):
        mod.write(path, [{'id': 1}])
