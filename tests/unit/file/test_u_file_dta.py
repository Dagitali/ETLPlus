"""
:mod:`tests.unit.file.test_u_file_dta` module.

Unit tests for :mod:`etlplus.file.dta`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import dta as mod
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from tests.unit.file.conftest import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: TESTS ============================================================ #


class TestDta:
    """Unit tests for :mod:`etlplus.file.dta`."""

    @pytest.fixture
    def handler(self) -> mod.DtaFile:
        """Create a DTA handler instance."""
        return mod.DtaFile()

    def test_uses_single_dataset_scientific_abc(self) -> None:
        """Test DTA handler base contract."""
        assert issubclass(mod.DtaFile, SingleDatasetScientificFileHandlerABC)
        assert mod.DtaFile.dataset_key == 'data'

    def test_rejects_non_default_dataset_key(
        self,
        handler: mod.DtaFile,
    ) -> None:
        """Test DTA rejecting non-default dataset key overrides."""
        assert_single_dataset_rejects_non_default_key(
            handler,
            suffix='dta',
        )

    def test_write_empty_payload_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test DTA write returning zero for empty payloads."""
        path = tmp_path / 'data.dta'
        assert mod.write(path, []) == 0
        assert not path.exists()
