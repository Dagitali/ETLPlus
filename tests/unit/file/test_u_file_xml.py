"""
:mod:`tests.unit.file.test_u_file_xml` module.

Unit tests for :mod:`etlplus.file.xml`.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

from etlplus.file import xml as mod

# SECTION: CONTRACTS ======================================================== #


class XmlModuleContract:
    """Reusable contract suite for XML module read/write behavior."""

    module: ModuleType
    format_name: str
    root_tag: str = 'root'

    def format_path(
        self,
        tmp_path: Path,
        *,
        stem: str = 'data',
    ) -> Path:
        """Build a deterministic format-specific path."""
        return tmp_path / f'{stem}.{self.format_name}'

    def test_write_uses_root_tag_and_read_round_trip(
        self,
        tmp_path: Path,
    ) -> None:
        """Test XML write using explicit root tag and readable output."""
        path = self.format_path(tmp_path)

        written = self.module.write(
            path,
            [{'id': 1}],
            root_tag=self.root_tag,
        )

        assert written == 1
        assert f'<{self.root_tag}>' in path.read_text(encoding='utf-8')
        result = self.module.read(path)
        assert self.root_tag in result

# SECTION: TESTS ============================================================ #


class TestXml(XmlModuleContract):
    """Unit tests for :mod:`etlplus.file.xml`."""

    module = mod
    format_name = 'xml'
    root_tag = 'rows'
