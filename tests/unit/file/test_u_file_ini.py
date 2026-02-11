"""
:mod:`tests.unit.file.test_u_file_ini` module.

Unit tests for :mod:`etlplus.file.ini`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import ini as mod
from tests.unit.file.conftest import SemiStructuredReadModuleContract
from tests.unit.file.conftest import SemiStructuredWriteDictModuleContract

# SECTION: TESTS ============================================================ #


class TestIni(
    SemiStructuredReadModuleContract,
    SemiStructuredWriteDictModuleContract,
):
    """Unit tests for :mod:`etlplus.file.ini`."""

    module = mod
    format_name = 'ini'
    sample_read_text = (
        '[DEFAULT]\n'
        'shared=base\n'
        'timeout=5\n'
        '\n'
        '[alpha]\n'
        'shared=override\n'
        'value=1\n'
        '\n'
        '[beta]\n'
        'value=2\n'
    )
    expected_read_payload = {
        'DEFAULT': {'shared': 'base', 'timeout': '5'},
        'alpha': {'value': '1'},
        'beta': {'value': '2'},
    }
    dict_payload = {
        'DEFAULT': {'shared': 'base', 'timeout': 5},
        'alpha': {'value': 1},
    }

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert INI write contract via read-back normalization."""
        reloaded = mod.IniFile().read(path)
        assert isinstance(reloaded, dict)
        assert reloaded['DEFAULT'] == {'shared': 'base', 'timeout': '5'}
        assert reloaded['alpha'] == {'value': '1'}

    def test_write_rejects_non_dict_default(
        self,
        tmp_path: Path,
    ) -> None:
        path = self.format_path(tmp_path, stem='config')

        with pytest.raises(
            TypeError,
            match='INI DEFAULT section must be a dict',
        ):
            mod.IniFile().write(path, {'DEFAULT': 'nope'})

    def test_write_rejects_non_dict_section(
        self,
        tmp_path: Path,
    ) -> None:
        path = self.format_path(tmp_path, stem='config')

        with pytest.raises(TypeError, match='INI sections must map to dicts'):
            mod.IniFile().write(path, {'alpha': 'nope'})
