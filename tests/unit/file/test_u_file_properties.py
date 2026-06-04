"""
:mod:`tests.unit.file.test_u_file_properties` module.

Unit tests for :mod:`etlplus.file.properties`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import cast

import pytest

from etlplus.file import properties as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contracts import SemiStructuredReadModuleContract
from .pytest_file_contracts import SemiStructuredWriteDictModuleContract
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestProperties(
    RoundtripUnitModuleContract,
    SemiStructuredReadModuleContract,
    SemiStructuredWriteDictModuleContract,
):
    """Unit tests for :mod:`etlplus.file.properties`."""

    module = mod
    format_name = 'properties'
    sample_read_text = (
        '# comment\n'
        '! another comment\n'
        'host=localhost\n'
        'port: 5432\n'
        'flag\n'
        '=ignored\n'
        ' spaced = value \n'
    )
    expected_read_payload = {
        'host': 'localhost',
        'port': '5432',
        'flag': '',
        'spaced': 'value',
    }
    dict_payload = {'b': 2, 'a': 1}
    roundtrip_spec = build_roundtrip_spec(
        {'b': 2, 'a': 1},
        {'a': '1', 'b': '2'},
    )

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert PROPERTIES writes sorted keys in output."""
        assert path.read_text(encoding='utf-8') == 'a=1\nb=2\n'

    def test_encode_dict_payload_text_stringifies_none_values(self) -> None:
        """PROPERTIES serialization should stringify ``None`` as an empty value."""
        assert (
            mod.PropertiesFile().encode_dict_payload_text(
                {
                    'empty': None,
                    'enabled': True,
                },
            )
            == 'empty=\nenabled=True\n'
        )

    @pytest.mark.parametrize(
        ('line', 'index', 'expected'),
        [
            (r'key\=name=value', 4, True),
            (r'key\\=name=value', 5, False),
            ('key=value', 3, False),
        ],
    )
    def test_is_escaped_counts_preceding_backslashes(
        self,
        line: str,
        index: int,
        expected: bool,
    ) -> None:
        """Test escape detection for odd and even backslash runs."""
        assert mod._is_escaped(line, index) is expected

    @pytest.mark.parametrize(
        ('line', 'expected'),
        [
            (r'message=hello\\', False),
            ('message=hello\\', True),
            ('message=hello', False),
        ],
    )
    def test_is_logical_line_continued_counts_trailing_backslashes(
        self,
        line: str,
        expected: bool,
    ) -> None:
        """Test continuation detection for trailing backslash runs."""
        assert mod._is_logical_line_continued(line) is expected

    def test_iter_logical_property_lines_flushes_dangling_continuation(
        self,
    ) -> None:
        """Dangling continuations should keep the pending logical line."""
        assert mod._iter_logical_property_lines('message=hello \\') == [
            'message=hello ',
        ]

    def test_parse_properties_text_skips_blank_and_comment_lines(
        self,
    ) -> None:
        """Blank and comment-only PROPERTIES lines should not emit payload keys."""
        assert mod._parse_properties_text(
            '\n   \n# comment\n! comment\nhost=localhost\n',
        ) == {'host': 'localhost'}

    def test_read_accepts_whitespace_separated_properties(
        self,
        tmp_path: Path,
    ) -> None:
        """Test whitespace-separated PROPERTIES entries parse as key-value rows."""
        path = self.format_path(tmp_path, stem='whitespace')
        path.write_text(
            'path /srv/app\n'
            'timeout   30\n'
            r'escaped\ key=value'
            '\n',
            encoding='utf-8',
        )

        assert mod.PropertiesFile().read(path) == {
            'path': '/srv/app',
            'timeout': '30',
            r'escaped\ key': 'value',
        }

    def test_read_joins_continued_properties_lines(
        self,
        tmp_path: Path,
    ) -> None:
        """Test Java-style PROPERTIES line continuations."""
        path = self.format_path(tmp_path, stem='continued')
        path.write_text(
            'message=hello \\\n    world\npath=/srv/app\n',
            encoding='utf-8',
        )

        assert mod.PropertiesFile().read(path) == {
            'message': 'hello world',
            'path': '/srv/app',
        }

    def test_read_keeps_property_after_continued_comment_line(
        self,
        tmp_path: Path,
    ) -> None:
        """Test comment lines ending in backslash do not consume properties."""
        path = self.format_path(tmp_path, stem='comment-continuation')
        path.write_text(
            '# ignored comment \\\nhost=localhost\n',
            encoding='utf-8',
        )

        assert mod.PropertiesFile().read(path) == {'host': 'localhost'}

    @pytest.mark.parametrize(
        ('line', 'expected'),
        [
            ('path /srv/app', ('path', '/srv/app')),
            ('path\t/srv/app', ('path', '/srv/app')),
            ('path   = /srv/app', ('path', '/srv/app')),
            ('path   : /srv/app', ('path', '/srv/app')),
            (r'escaped\ key=value', (r'escaped\ key', 'value')),
            (r'escaped\:key=value', (r'escaped\:key', 'value')),
            (r'escaped\=key:value', (r'escaped\=key', 'value')),
            ('flag', ('flag', '')),
        ],
    )
    def test_split_key_value_handles_java_style_separators(
        self,
        line: str,
        expected: tuple[str, str],
    ) -> None:
        """Test Java-style PROPERTIES separators without changing the API."""
        assert mod._split_key_value(line) == expected

    def test_write_rejects_non_dict(
        self,
        tmp_path: Path,
    ) -> None:
        path = self.format_path(tmp_path, stem='config')

        with pytest.raises(TypeError, match='PROPERTIES'):
            mod.PropertiesFile().write(path, cast(Any, ['nope']))
