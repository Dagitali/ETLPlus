"""
:mod:`tests.unit.file.test_u_file_xpt` module.

Unit tests for :mod:`etlplus.file.xpt`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import xpt as mod
from etlplus.file.base import SingleDatasetScientificFileHandlerABC
from tests.unit.file.conftest import (
    assert_single_dataset_rejects_non_default_key,
)

# SECTION: TESTS ============================================================ #


class TestXpt:
    """Unit tests for :mod:`etlplus.file.xpt`."""

    @pytest.fixture
    def handler(self) -> mod.XptFile:
        """Create a XPT handler instance."""
        return mod.XptFile()

    def test_uses_single_dataset_scientific_abc(self) -> None:
        """Test XPT handler base contract."""
        assert issubclass(mod.XptFile, SingleDatasetScientificFileHandlerABC)
        assert mod.XptFile.dataset_key == 'data'

    def test_rejects_non_default_dataset_key(
        self,
        handler: mod.XptFile,
    ) -> None:
        """Test XPT rejecting non-default dataset key overrides."""
        assert_single_dataset_rejects_non_default_key(
            handler,
            suffix='xpt',
        )

    def test_write_empty_payload_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """Test XPT write returning zero for empty payloads."""
        path = tmp_path / 'data.xpt'
        assert mod.write(path, []) == 0
        assert not path.exists()
