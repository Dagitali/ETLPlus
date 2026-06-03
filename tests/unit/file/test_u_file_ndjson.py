"""
:mod:`tests.unit.file.test_u_file_ndjson` module.

Unit tests for :mod:`etlplus.file.ndjson`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from etlplus.file import ndjson as mod

from .pytest_file_contract_mixins import JsonLinesWriteContractMixin
from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_contracts import SemiStructuredReadModuleContract
from .pytest_file_contracts import SemiStructuredWriteDictModuleContract
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestNdjson(
    JsonLinesWriteContractMixin,
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
    json_lines_error_pattern = 'NDJSON payloads must contain'
    roundtrip_spec = build_roundtrip_spec(record_count=2)

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert NDJSON dict payload serialization."""
        assert path.read_text(encoding='utf-8').strip() == '{"id": 1}'

    def test_load_line_parses_one_record(self) -> None:
        """Test that :func:`load_line` parses one JSON object line."""
        assert mod.NdjsonFile().load_line('{"id": 1}') == {'id': 1}

    def test_load_line_rejects_blank_input(self) -> None:
        """Test that :func:`load_line` rejects blank lines."""
        with pytest.raises(ValueError, match='cannot be blank'):
            mod.NdjsonFile().load_line('   ')

    @pytest.mark.parametrize(
        ('content', 'error_type', 'match'),
        [
            (
                '{"id": 1}\n{broken\n',
                json.JSONDecodeError,
                None,
            ),
            (
                '{"id": 1}\n42\n',
                TypeError,
                'line 2',
            ),
        ],
    )
    def test_read_rejects_invalid_lines(
        self,
        tmp_path: Path,
        content: str,
        error_type: type[Exception],
        match: str | None,
    ) -> None:
        """Test that reads reject invalid JSON or non-object lines."""
        path = self.format_path(tmp_path)
        path.write_text(content, encoding='utf-8')

        with pytest.raises(error_type, match=match):
            mod.NdjsonFile().read(path)

    def test_dump_line_serializes_one_record_with_newline(self) -> None:
        """Test that :func:`dump_line` emits one NDJSON line."""
        assert mod.NdjsonFile().dump_line({'id': 1}) == '{"id": 1}\n'
