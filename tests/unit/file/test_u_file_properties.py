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
        {'b': 2, 'a': 1}, {'a': '1', 'b': '2'},
    )

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert PROPERTIES writes sorted keys in output."""
        assert path.read_text(encoding='utf-8') == 'a=1\nb=2\n'

    def test_write_rejects_non_dict(
        self,
        tmp_path: Path,
    ) -> None:
        path = self.format_path(tmp_path, stem='config')

        with pytest.raises(TypeError, match='PROPERTIES'):
            mod.PropertiesFile().write(path, cast(dict[str, Any], ['nope']))
