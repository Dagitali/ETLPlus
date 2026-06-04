"""
:mod:`tests.unit.file.test_u_file_ini` module.

Unit tests for :mod:`etlplus.file.ini`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import ini as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contracts import SemiStructuredReadModuleContract
from .pytest_file_contracts import SemiStructuredWriteDictModuleContract
from .pytest_file_roundtrip_cases import ROUNDTRIP_CASES
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestIni(
    RoundtripUnitModuleContract,
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
    roundtrip_spec = build_roundtrip_spec(
        *ROUNDTRIP_CASES['ini_default_alpha'],
    )

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert INI write contract via read-back normalization."""
        reloaded = mod.IniFile().read(path)
        assert isinstance(reloaded, dict)
        assert reloaded['DEFAULT'] == {'shared': 'base', 'timeout': '5'}
        assert reloaded['alpha'] == {'value': '1'}

    def test_loads_without_default_section_omits_default_key(self) -> None:
        """
        Test that INI parsing without defaults omitting the ``DEFAULT`` key.
        """
        payload = mod.IniFile().loads('[alpha]\nvalue=1\n')
        assert payload == {'alpha': {'value': '1'}}

    @pytest.mark.parametrize(
        ('payload', 'match'),
        [
            ({'DEFAULT': 'nope'}, 'INI DEFAULT section must be a dict'),
            ({'alpha': 'nope'}, 'INI sections must map to dicts'),
        ],
    )
    def test_write_rejects_non_dict_sections(
        self,
        tmp_path: Path,
        payload: dict[str, object],
        match: str,
    ) -> None:
        path = self.format_path(tmp_path, stem='config')

        with pytest.raises(TypeError, match=match):
            mod.IniFile().write(path, payload)
