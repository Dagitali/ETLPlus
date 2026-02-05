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

# SECTION: TESTS ============================================================ #


class TestPropertiesRead:
    """Unit tests for :func:`etlplus.file.properties.read`."""

    def test_read_parses_keys_and_ignores_comments(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`read` correctly parses key-value pairs and ignores
        comments.
        """
        path = tmp_path / 'config.properties'
        path.write_text(
            '# comment\n'
            '! another comment\n'
            'host=localhost\n'
            'port: 5432\n'
            'flag\n'
            '=ignored\n'
            ' spaced = value \n',
            encoding='utf-8',
        )

        result = mod.read(path)

        assert result == {
            'host': 'localhost',
            'port': '5432',
            'flag': '',
            'spaced': 'value',
        }


class TestPropertiesWrite:
    """Unit tests for :func:`etlplus.file.properties.write`."""

    def test_write_sorts_keys(
        self,
        tmp_path: Path,
    ) -> None:
        """T
        Test that :func:`write` sorts keys alphabetically in the output file.
        """
        path = tmp_path / 'config.properties'
        payload = {'b': 2, 'a': 1}

        written = mod.write(path, payload)

        assert written == 1
        assert path.read_text(encoding='utf-8') == 'a=1\nb=2\n'

    def test_write_rejects_non_dict(
        self,
        tmp_path: Path,
    ) -> None:
        path = tmp_path / 'config.properties'

        with pytest.raises(TypeError, match='PROPERTIES'):
            mod.write(path, cast(dict[str, Any], ['nope']))
