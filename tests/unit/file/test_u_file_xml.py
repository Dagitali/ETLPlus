"""
:mod:`tests.unit.file.test_u_file_xml` module.

Unit tests for :mod:`etlplus.file.xml`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.file import xml as mod
from etlplus.file.base import WriteOptions

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_roundtrip_cases import ROUNDTRIP_CASES
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestXml(RoundtripUnitModuleContract):
    """Unit tests for :mod:`etlplus.file.xml`."""

    module = mod
    format_name = 'xml'
    root_tag = 'rows'
    roundtrip_spec = build_roundtrip_spec(
        *ROUNDTRIP_CASES['xml_nested_attributes'],
    )

    def test_dict_to_element_allows_none_payload(self) -> None:
        """Test that helper creates an empty element for ``None`` payloads."""
        element = mod._dict_to_element('node', None)
        assert element.tag == 'node'
        assert element.text is None
        assert not list(element)

    def test_dumps_defaults_to_root_tag_for_non_single_mapping(self) -> None:
        """
        Test that :meth:`dumps` uses the default root tag for list payloads.
        """
        text = mod.XmlFile().dumps([{'id': 1}])

        assert text.startswith('<root>')
        assert '<item>' in text

    def test_dumps_prefers_single_mapping_root_over_options(self) -> None:
        """Test that :meth:`dumps` preserves explicit single mapping roots."""
        text = mod.XmlFile().dumps(
            {'rows': [{'id': 1}]},
            options=WriteOptions(root_tag='ignored'),
        )

        assert text.startswith('<rows>')
        assert 'ignored' not in text

    def test_loads_parses_attributes_and_repeated_tags(self) -> None:
        """
        Test that :meth:`loads` converts attributes and repeated tags
        predictably.
        """
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

    def test_loads_three_repeated_tags_appends_to_existing_list(self) -> None:
        """Test that repeated XML tags append when a list already exists."""
        payload = (
            '<root>'
            '<item><text>first</text></item>'
            '<item><text>second</text></item>'
            '<item><text>third</text></item>'
            '</root>'
        )

        result = mod.XmlFile().loads(payload)

        assert result == {
            'root': {
                'item': [
                    {'text': {'text': 'first'}},
                    {'text': {'text': 'second'}},
                    {'text': {'text': 'third'}},
                ],
            },
        }

    def test_write_uses_root_tag_and_read_roundtrip(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that XML write using explicit root tag and readable output."""
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
