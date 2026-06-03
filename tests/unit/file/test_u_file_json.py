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
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

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
    roundtrip_spec = build_roundtrip_spec(record_count=2)

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

    @pytest.mark.parametrize(
        ('content', 'match'),
        [
            (
                json.dumps([1, 2]),
                'JSON array must contain',
            ),
            ('42', 'JSON root must be'),
        ],
    )
    def test_read_rejects_invalid_root(
        self,
        tmp_path: Path,
        content: str,
        match: str,
    ) -> None:
        """Test that :func:`read` rejects unsupported JSON root values."""
        path = self.format_path(tmp_path)
        path.write_text(content, encoding='utf-8')

        with pytest.raises(TypeError, match=match):
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
