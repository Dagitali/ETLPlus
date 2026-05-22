"""
:mod:`tests.unit.file.test_u_file_log` module.

Unit tests for :mod:`etlplus.file.log`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from etlplus.file import log as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_roundtrip_cases import build_roundtrip_spec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestLog(RoundtripUnitModuleContract):
    """Unit tests for :mod:`etlplus.file.log`."""

    module = mod
    format_name = 'log'
    roundtrip_spec = build_roundtrip_spec(record_count=2)

    @pytest.mark.parametrize(
        ('line', 'parsed_payload', 'expected_message'),
        [
            pytest.param('["a", "b"]', None, '["a", "b"]', id='non-object-json'),
            pytest.param('plain message', None, 'plain message', id='plain-text'),
            pytest.param('ignored', ['a', 'b'], 'ignored', id='non-mapping-parse'),
        ],
    )
    def test_parse_line_falls_back_to_message(
        self,
        monkeypatch: pytest.MonkeyPatch,
        line: str,
        parsed_payload: object | None,
        expected_message: str,
    ) -> None:
        """Test fallback behavior for plain or parsed non-object log lines."""
        if parsed_payload is not None:
            monkeypatch.setattr(mod.JsonCodec, 'parse', lambda _: parsed_payload)

        assert self.module_handler.parse_line(line) == {'message': expected_message}

    def test_parse_line_parses_json_object(self) -> None:
        """Test that the line parser returns JSON objects as events."""
        assert self.module_handler.parse_line('{"id": 1}') == {'id': 1}

    def test_read_skips_blank_lines_and_parses_entries(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that reads skipping blanks and parsing each non-empty line."""
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
        """Test that :meth:`write` rejects non-object array payloads."""
        path = self.format_path(tmp_path)
        invalid_payload: Any = [1]

        with pytest.raises(TypeError, match='LOG payloads must contain'):
            self.module_handler.write(path, invalid_payload)

    def test_write_returns_zero_for_empty_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :meth:`write` returns zero for empty payload without creating
        a file.
        """
        path = self.format_path(tmp_path)

        assert self.module_handler.write(path, []) == 0
        assert not path.exists()

    def test_write_serializes_json_lines_and_newline(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :meth:`write` produces one JSON event per line."""
        path = self.format_path(tmp_path)
        payload = [{'id': 1}, {'id': 2}]

        written = self.module_handler.write(path, payload)

        assert written == 2
        text = path.read_text(encoding='utf-8')
        assert text.endswith('\n')
        lines = text.splitlines()
        assert [json.loads(line) for line in lines] == payload
