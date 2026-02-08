"""
:mod:`tests.unit.file.test_u_file_mat` module.

Unit tests for :mod:`etlplus.file.mat`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import mat as mod
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from tests.unit.file._module_contracts import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: TESTS ============================================================ #


class TestMat:
    """Unit tests for :mod:`etlplus.file.mat`."""

    @pytest.fixture
    def handler(self) -> mod.MatFile:
        """Create a MAT handler instance."""
        return mod.MatFile()

    def test_uses_single_dataset_scientific_abc(self) -> None:
        """Test MAT handler base contract."""
        assert issubclass(mod.MatFile, SingleDatasetScientificFileHandlerABC)
        assert mod.MatFile.dataset_key == 'data'

    def test_rejects_non_default_dataset_key(
        self,
        handler: mod.MatFile,
    ) -> None:
        """Test MAT rejecting non-default dataset key overrides."""
        assert_single_dataset_rejects_non_default_key(
            handler,
            suffix='mat',
        )

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_level_placeholders_raise_not_implemented(
        self,
        tmp_path: Path,
        operation: str,
    ) -> None:
        """Test MAT module-level placeholder read/write behavior."""
        path = tmp_path / 'data.mat'
        with pytest.raises(NotImplementedError, match='not implemented yet'):
            if operation == 'read':
                mod.read(path)
            else:
                mod.write(path, [{'id': 1}])
