"""
:mod:`tests.unit.file.test_u_file_zsav` module.

Unit tests for :mod:`etlplus.file.zsav`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import zsav as mod
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from tests.unit.file._module_contracts import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: TESTS ============================================================ #


class TestZsav:
    """Unit tests for :mod:`etlplus.file.zsav`."""

    @pytest.fixture
    def handler(self) -> mod.ZsavFile:
        """Create a ZSAV handler instance."""
        return mod.ZsavFile()

    def test_uses_single_dataset_scientific_abc(self) -> None:
        """Test ZSAV handler base contract."""
        assert issubclass(mod.ZsavFile, SingleDatasetScientificFileHandlerABC)
        assert mod.ZsavFile.dataset_key == 'data'

    def test_rejects_non_default_dataset_key(
        self,
        handler: mod.ZsavFile,
    ) -> None:
        """Test ZSAV rejecting non-default dataset key overrides."""
        assert_single_dataset_rejects_non_default_key(
            handler,
            suffix='zsav',
        )

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_level_placeholders_raise_not_implemented(
        self,
        tmp_path: Path,
        operation: str,
    ) -> None:
        """Test zsav module-level placeholder read/write behavior."""
        path = tmp_path / 'data.zsav'
        with pytest.raises(NotImplementedError, match='not implemented yet'):
            if operation == 'read':
                mod.read(path)
            else:
                mod.write(path, [{'id': 1}])
