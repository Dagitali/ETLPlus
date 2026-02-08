"""
:mod:`tests.unit.file.test_u_file_sav` module.

Unit tests for :mod:`etlplus.file.sav`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import sav as mod
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from tests.unit.file._module_contracts import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: TESTS ============================================================ #


class TestSav:
    """Unit tests for :mod:`etlplus.file.sav`."""

    @pytest.fixture
    def handler(self) -> mod.SavFile:
        """Create a SAV handler instance."""
        return mod.SavFile()

    def test_uses_single_dataset_scientific_abc(self) -> None:
        """Test SAV handler base contract."""
        assert issubclass(mod.SavFile, SingleDatasetScientificFileHandlerABC)
        assert mod.SavFile.dataset_key == 'data'

    def test_rejects_non_default_dataset_key(
        self,
        handler: mod.SavFile,
    ) -> None:
        """Test SAV rejecting non-default dataset key overrides."""
        assert_single_dataset_rejects_non_default_key(
            handler,
            suffix='sav',
        )

    def test_write_empty_payload_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test SAV write returning zero for empty payloads."""
        path = tmp_path / 'data.sav'
        assert mod.write(path, []) == 0
        assert not path.exists()
