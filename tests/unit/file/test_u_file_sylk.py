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


class TestSylk:
    """Unit tests for :mod:`etlplus.file.sylk`."""

    @pytest.fixture
    def handler(self) -> mod.SylkFile:
        """Create a SYLK handler instance."""
        return mod.SylkFile()

    def test_uses_single_dataset_scientific_abc(self) -> None:
        """Test SYLK handler base contract."""
        assert issubclass(mod.SylkFile, SingleDatasetScientificFileHandlerABC)
        assert mod.SylkFile.dataset_key == 'data'

    def test_rejects_non_default_dataset_key(
        self,
        handler: mod.SylkFile,
    ) -> None:
        """Test SYLK rejecting non-default dataset key overrides."""
        assert_single_dataset_rejects_non_default_key(
            handler,
            suffix='sylk',
        )

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_level_placeholders_raise_not_implemented(
        self,
        tmp_path: Path,
        operation: str,
    ) -> None:
        """Test SYLK module-level placeholder read/write behavior."""
        path = tmp_path / 'data.sylk'
        with pytest.raises(NotImplementedError, match='not implemented yet'):
            if operation == 'read':
                mod.read(path)
            else:
                mod.write(path, [{'id': 1}])
