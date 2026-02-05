"""
:mod:`tests.unit.file.test_u_file_ini` module.

Unit tests for :mod:`etlplus.file.ini`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import ini as mod

# SECTION: TESTS ============================================================ #


class TestIniRead:
    """Unit tests for :func:`etlplus.file.ini.read`."""

    def test_read_includes_default_and_strips_defaults_from_sections(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`read` includes DEFAULT values and strips sections.
        """
        path = tmp_path / 'config.ini'
        path.write_text(
            '[DEFAULT]\n'
            'shared=base\n'
            'timeout=5\n'
            '\n'
            '[alpha]\n'
            'shared=override\n'
            'value=1\n'
            '\n'
            '[beta]\n'
            'value=2\n',
            encoding='utf-8',
        )

        result = mod.read(path)

        assert isinstance(result, dict)
        assert result['DEFAULT'] == {'shared': 'base', 'timeout': '5'}
        assert result['alpha'] == {'value': '1'}
        assert result['beta'] == {'value': '2'}


class TestIniWrite:
    """Unit tests for :func:`etlplus.file.ini.write`."""

    def test_write_rejects_non_dict_default(
        self,
        tmp_path: Path,
    ) -> None:
        path = tmp_path / 'config.ini'

        with pytest.raises(
            TypeError, match='INI DEFAULT section must be a dict',
        ):
            mod.write(path, {'DEFAULT': 'nope'})

    def test_write_rejects_non_dict_section(
        self,
        tmp_path: Path,
    ) -> None:
        path = tmp_path / 'config.ini'

        with pytest.raises(TypeError, match='INI sections must map to dicts'):
            mod.write(path, {'alpha': 'nope'})

    def test_write_round_trip(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing and reading an INI file round-trips correctly."""
        path = tmp_path / 'config.ini'
        payload = {
            'DEFAULT': {'shared': 'base', 'timeout': 5},
            'alpha': {'value': 1},
        }

        written = mod.write(path, payload)

        assert written == 1
        reloaded = mod.read(path)
        assert isinstance(reloaded, dict)
        assert reloaded['DEFAULT'] == {'shared': 'base', 'timeout': '5'}
        assert reloaded['alpha'] == {'value': '1'}
