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
from tests.unit.file.conftest import SemiStructuredReadModuleContract
from tests.unit.file.conftest import SemiStructuredWriteDictModuleContract

# SECTION: TESTS ============================================================ #


class TestProperties(
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
        path = tmp_path / 'config.properties'

        with pytest.raises(TypeError, match='PROPERTIES'):
            mod.write(path, cast(dict[str, Any], ['nope']))
