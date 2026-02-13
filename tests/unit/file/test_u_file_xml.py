"""
:mod:`tests.unit.file.test_u_file_xml` module.

Unit tests for :mod:`etlplus.file.xml`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import xml as mod
from etlplus.file.base import WriteOptions
from tests.unit.file.conftest import RoundtripSpec
from tests.unit.file.conftest import RoundtripUnitModuleContract

# SECTION: TESTS ============================================================ #


class TestXml(RoundtripUnitModuleContract):
    """Unit tests for :mod:`etlplus.file.xml`."""

    module = mod
    format_name = 'xml'
    root_tag = 'rows'
    roundtrip_spec = RoundtripSpec(
        payload={
            'root': {
                '@id': '42',
                'item': {'@lang': 'en', 'text': 'Hello'},
            },
        },
        expected={
            'root': {
                '@id': '42',
                'item': {'@lang': 'en', 'text': 'Hello'},
            },
        },
    )

    def test_write_uses_root_tag_and_read_roundtrip(
        self,
        tmp_path: Path,
    ) -> None:
        """Test XML write using explicit root tag and readable output."""
        path = self.format_path(tmp_path)

        written = self.module_handler.write(
            path,
            [{'id': 1}],
            options=WriteOptions(root_tag=self.root_tag),
        )

        assert written == 1
        assert f'<{self.root_tag}>' in path.read_text(encoding='utf-8')
        result = self.module_handler.read(path)
        assert self.root_tag in result

    def test_dumps_defaults_to_root_tag_for_non_single_mapping(self) -> None:
        """Test dumps using the default root tag for list payloads."""
        text = mod.XmlFile().dumps([{'id': 1}])

        assert text.startswith('<root>')
        assert '<item>' in text

    def test_dumps_prefers_single_mapping_root_over_options(self) -> None:
        """Test dumps preserving explicit single mapping roots."""
        text = mod.XmlFile().dumps(
            {'rows': [{'id': 1}]},
            options=WriteOptions(root_tag='ignored'),
        )

        assert text.startswith('<rows>')
        assert 'ignored' not in text

    def test_loads_parses_attributes_and_repeated_tags(self) -> None:
        """Test loads converting attributes and repeated tags predictably."""
        payload = (
            '<root id="7">'
            '<item code="A"><text>first</text></item>'
            '<item code="B"><text>second</text></item>'
            '</root>'
        )

        result = mod.XmlFile().loads(payload)

        assert result == {
            'root': {
                '@id': '7',
                'item': [
                    {'@code': 'A', 'text': {'text': 'first'}},
                    {'@code': 'B', 'text': {'text': 'second'}},
                ],
            },
        }
