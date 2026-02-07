"""
:mod:`tests.unit.file.test_u_file_xml` module.

Unit tests for :mod:`etlplus.file.xml`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import xml as mod

# SECTION: TESTS ============================================================ #


class TestXmlReadWrite:
    """Unit tests for XML read/write functions."""

    def test_write_uses_root_tag_and_read_round_trip(
        self,
        tmp_path: Path,
    ) -> None:
        """Test XML write using explicit root_tag and readable output."""
        path = tmp_path / 'data.xml'

        written = mod.write(path, [{'id': 1}], root_tag='rows')

        assert written == 1
        assert '<rows>' in path.read_text(encoding='utf-8')
        result = mod.read(path)
        assert 'rows' in result
