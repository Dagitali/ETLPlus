"""
:mod:`tests.unit.file.test_u_file_json` module.

Unit tests for :mod:`etlplus.file.json`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from etlplus.file import json as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contracts import SemiStructuredReadModuleContract
from .pytest_file_contracts import SemiStructuredWriteDictModuleContract
from .pytest_file_roundtrip_cases import build_two_id_records_roundtrip_spec

# SECTION: TESTS ============================================================ #


class TestJson(
    RoundtripUnitModuleContract,
    SemiStructuredReadModuleContract,
    SemiStructuredWriteDictModuleContract,
):
    """Unit tests for :mod:`etlplus.file.json`."""

    module = mod
    format_name = 'json'
    sample_read_text = json.dumps({'id': 1})
    expected_read_payload = {'id': 1}
    dict_payload = {'id': 1}
    roundtrip_spec = build_two_id_records_roundtrip_spec()

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Test writing a dict payload including trailing newline."""
        content = path.read_text(encoding='utf-8')
        assert content.endswith('\n')
        assert json.loads(content) == self.dict_payload

    def test_read_list_of_records(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`read` correctly reads a list of JSON objects as
        records.
        """
        path = self.format_path(tmp_path)
        path.write_text(json.dumps([{'id': 1}]), encoding='utf-8')

        assert mod.JsonFile().read(path) == [{'id': 1}]

    def test_read_rejects_non_object_root(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`read` rejects a JSON array as root since it cannot be
        treated as records.
        """
        path = self.format_path(tmp_path)
        path.write_text(json.dumps([1, 2]), encoding='utf-8')

        with pytest.raises(TypeError, match='JSON array must contain'):
            mod.JsonFile().read(path)

    def test_read_rejects_scalar_root(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`read` rejects a scalar JSON value as root."""
        path = self.format_path(tmp_path)
        path.write_text('42', encoding='utf-8')

        with pytest.raises(TypeError, match='JSON root must be'):
            mod.JsonFile().read(path)

    def test_write_adds_newline_and_counts_records(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`write` adds a newline and counts records correctly.
        """
        path = self.format_path(tmp_path)
        payload = [{'id': 1}, {'id': 2}]

        written = mod.JsonFile().write(path, payload)

        assert written == 2
        content = path.read_text(encoding='utf-8')
        assert content.endswith('\n')
