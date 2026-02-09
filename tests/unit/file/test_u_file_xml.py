"""
:mod:`tests.unit.file.test_u_file_xml` module.

Unit tests for :mod:`etlplus.file.xml`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import xml as mod
from tests.unit.file.conftest import PathMixin

# SECTION: TESTS ============================================================ #


class TestXml(PathMixin):
    """Unit tests for :mod:`etlplus.file.xml`."""

    module = mod
    format_name = 'xml'
    root_tag = 'rows'

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
