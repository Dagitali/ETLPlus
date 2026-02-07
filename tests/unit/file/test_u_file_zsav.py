"""
:mod:`tests.unit.file.test_u_file_zsav` module.

Unit tests for :mod:`etlplus.file.zsav`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import zsav as mod
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from tests.unit.file.conftest import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: TESTS ============================================================ #


def test_zsav_uses_single_dataset_scientific_abc() -> None:
    """Test zsav handler base contract."""
    assert issubclass(mod.ZsavFile, SingleDatasetScientificFileHandlerABC)
    assert mod.ZsavFile.dataset_key == 'data'


def test_zsav_rejects_non_default_dataset_key() -> None:
    """Test zsav rejecting non-default dataset key overrides."""
    assert_single_dataset_rejects_non_default_key(
        mod.ZsavFile(),
        suffix='zsav',
    )


def test_zsav_read_write_raise_not_implemented(
    tmp_path: Path,
) -> None:
    """Test zsav module-level read/write placeholder behavior."""
    path = tmp_path / 'data.zsav'
    with pytest.raises(NotImplementedError, match='not implemented yet'):
        mod.read(path)
    with pytest.raises(NotImplementedError, match='not implemented yet'):
        mod.write(path, [{'id': 1}])
