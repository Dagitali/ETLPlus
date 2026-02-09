"""
:mod:`tests.unit.file.test_u_file_arrow` module.

Unit tests for :mod:`etlplus.file.arrow`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import arrow as mod
from tests.unit.file.conftest import PyarrowMissingDependencyMixin

# SECTION: TESTS ============================================================ #


class TestArrow(PyarrowMissingDependencyMixin):
    """Unit tests for :mod:`etlplus.file.arrow`."""

    module = mod
    format_name = 'arrow'
    missing_dependency_pattern = 'missing pyarrow'

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test empty writes short-circuiting without file creation."""
        path = self.format_path(tmp_path)
        assert self.module.write(path, []) == 0
        assert not path.exists()
