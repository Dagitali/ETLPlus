"""
:mod:`tests.unit.file.test_u_file_log` module.

Unit tests for :mod:`etlplus.file.log`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from etlplus.file import log as mod
from etlplus.types import JSONData

from .pytest_file_contract_mixins import RoundtripSpec
from .pytest_file_contract_mixins import RoundtripUnitModuleContract

# SECTION: TESTS ============================================================ #


class TestLog(RoundtripUnitModuleContract):
    """Unit tests for :mod:`etlplus.file.log`."""

    module = mod
    format_name = 'log'
    roundtrip_spec = RoundtripSpec(
        payload=[{'id': 1}, {'id': 2}],
        expected=[{'id': 1}, {'id': 2}],
    )

    def test_parse_line_falls_back_for_non_object_json(self) -> None:
        """Test line parser falling back for non-object JSON values."""
        assert self.module_handler.parse_line('["a", "b"]') == {
            'message': '["a", "b"]',
        }

    def test_parse_line_falls_back_to_message_for_plain_text(self) -> None:
        """Test line parser falling back for non-JSON log lines."""
        assert self.module_handler.parse_line('plain message') == {
            'message': 'plain message',
        }

    def test_parse_line_parses_json_object(self) -> None:
        """Test line parser returning JSON objects as events."""
        assert self.module_handler.parse_line('{"id": 1}') == {'id': 1}

    def test_read_skips_blank_lines_and_parses_entries(
        self,
        tmp_path: Path,
    ) -> None:
        """Test reads skipping blanks and parsing each non-empty line."""
        path = self.format_path(tmp_path)
        path.write_text(
            '{"id": 1}\n\nplain text\n{"id": 2}\n',
            encoding='utf-8',
        )

        assert self.module_handler.read(path) == [
            {'id': 1},
            {'message': 'plain text'},
            {'id': 2},
        ]

    def test_write_rejects_non_object_payload_list(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writes rejecting non-object array payloads."""
        path = self.format_path(tmp_path)

        with pytest.raises(TypeError, match='LOG payloads must contain'):
            self.module_handler.write(path, cast(JSONData, [1]))

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """Test empty write payload returning zero without creating a file."""
        path = self.format_path(tmp_path)

        assert self.module_handler.write(path, []) == 0
        assert not path.exists()

    def test_write_serializes_json_lines_and_newline(
        self,
        tmp_path: Path,
    ) -> None:
        """Test writes producing one JSON event per line."""
        path = self.format_path(tmp_path)
        payload = [{'id': 1}, {'id': 2}]

        written = self.module_handler.write(path, payload)

        assert written == 2
        text = path.read_text(encoding='utf-8')
        assert text.endswith('\n')
        lines = text.splitlines()
        assert [json.loads(line) for line in lines] == payload
