"""
:mod:`tests.unit.file.test_u_file_ndjson` module.

Unit tests for :mod:`etlplus.file.ndjson`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from typing import cast

import pytest

from etlplus.file import ndjson as mod
from tests.unit.file.pytest_file_contract_contracts import (
    SemiStructuredReadModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    SemiStructuredWriteDictModuleContract,
)
from tests.unit.file.pytest_file_contract_mixins import RoundtripSpec
from tests.unit.file.pytest_file_contract_mixins import (
    RoundtripUnitModuleContract,
)

# SECTION: TESTS ============================================================ #


class TestNdjson(
    RoundtripUnitModuleContract,
    SemiStructuredReadModuleContract,
    SemiStructuredWriteDictModuleContract,
):
    """Unit tests for :mod:`etlplus.file.ndjson`."""

    module = mod
    format_name = 'ndjson'
    sample_read_text = '{"id": 1}\n\n   \n{"id": 2}\n'
    expected_read_payload = [{'id': 1}, {'id': 2}]
    dict_payload = {'id': 1}
    roundtrip_spec = RoundtripSpec(
        payload=[{'id': 1}, {'id': 2}],
        expected=[{'id': 1}, {'id': 2}],
    )

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert NDJSON dict payload serialization."""
        assert path.read_text(encoding='utf-8').strip() == '{"id": 1}'

    def test_read_raises_for_invalid_json(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`read` raises a JSONDecodeError for lines that do not
        contain valid JSON.
        """
        path = self.format_path(tmp_path)
        path.write_text('{"id": 1}\n{broken\n', encoding='utf-8')

        with pytest.raises(json.JSONDecodeError):
            mod.NdjsonFile().read(path)

    def test_read_rejects_non_dict_line(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`read` rejects lines that do not contain JSON objects.
        """
        path = self.format_path(tmp_path)
        path.write_text(
            '{"id": 1}\n42\n',
            encoding='utf-8',
        )

        with pytest.raises(TypeError, match='line 2'):
            mod.NdjsonFile().read(path)

    def test_write_empty_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that writing an empty payload returns zero and creates no file.
        """
        path = self.format_path(tmp_path)

        assert mod.NdjsonFile().write(path, []) == 0
        assert not path.exists()

    def test_write_rejects_non_dict_records(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing rejects records that are not dictionaries."""
        path = self.format_path(tmp_path)

        with pytest.raises(TypeError, match='NDJSON payloads must contain'):
            mod.NdjsonFile().write(path, cast(list[dict[str, Any]], [1]))

    def test_write_writes_each_record_on_its_own_line(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that writing writes each record on its own line.
        """
        path = self.format_path(tmp_path)
        payload = [{'id': 1}, {'id': 2}]

        written = mod.NdjsonFile().write(path, payload)

        assert written == 2
        lines = path.read_text(encoding='utf-8').splitlines()
        assert [json.loads(line) for line in lines] == payload
