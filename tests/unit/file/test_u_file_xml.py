"""
:mod:`tests.unit.file.test_u_file_xml` module.

Unit tests for :mod:`etlplus.file.xml`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

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

    @pytest.mark.parametrize(
        ('expected', 'present'),
        [
            pytest.param('<root>', True, id='root-tag'),
            pytest.param('<item>', True, id='item-tag'),
        ],
    )
    def test_dumps_defaults_to_root_tag_for_non_single_mapping(
        self,
        expected: str,
        present: bool,
    ) -> None:
        """
        Test that :meth:`dumps` uses the default root tag for list payloads.
        """
        text = mod.XmlFile().dumps([{'id': 1}])

        assert (expected in text) is present

    @pytest.mark.parametrize(
        ('expected', 'present'),
        [
            pytest.param('<rows>', True, id='single-root'),
            pytest.param('ignored', False, id='option-root-ignored'),
        ],
    )
    def test_dumps_prefers_single_mapping_root_over_options(
        self,
        expected: str,
        present: bool,
    ) -> None:
        """Test that :meth:`dumps` preserves explicit single mapping roots."""
        text = mod.XmlFile().dumps(
            {'rows': [{'id': 1}]},
            options=WriteOptions(root_tag='ignored'),
        )

        assert (expected in text) is present

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            (
                (
                    '<root id="7">'
                    '<item code="A"><text>first</text></item>'
                    '<item code="B"><text>second</text></item>'
                    '</root>'
                ),
                {
                    'root': {
                        '@id': '7',
                        'item': [
                            {'@code': 'A', 'text': {'text': 'first'}},
                            {'@code': 'B', 'text': {'text': 'second'}},
                        ],
                    },
                },
            ),
            (
                (
                    '<root>'
                    '<item><text>first</text></item>'
                    '<item><text>second</text></item>'
                    '<item><text>third</text></item>'
                    '</root>'
                ),
                {
                    'root': {
                        'item': [
                            {'text': {'text': 'first'}},
                            {'text': {'text': 'second'}},
                            {'text': {'text': 'third'}},
                        ],
                    },
                },
            ),
        ],
    )
    def test_loads_normalizes_repeated_tags(
        self,
        payload: str,
        expected: dict[str, object],
    ) -> None:
        """Test repeated XML tag normalization variants."""
        assert mod.XmlFile().loads(payload) == expected

    def test_write_uses_root_tag_and_reads_back(
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
        assert self.root_tag in self.module_handler.read(path)
