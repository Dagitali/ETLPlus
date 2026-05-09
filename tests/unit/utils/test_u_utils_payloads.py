"""
:mod:`tests.unit.utils.test_u_utils_payloads` module.

Unit tests for generic payload helpers in :mod:`etlplus.utils._payloads`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import etlplus.utils._payloads as payload_mod
from etlplus.utils import JsonCodec

# SECTION: TESTS ============================================================ #


class TestInferPayloadFormat:
    @pytest.mark.parametrize(
        ('raw', 'expected'),
        [
            pytest.param(' {"a":1}', 'json', id='json'),
            pytest.param('  col1,col2', 'csv', id='csv'),
        ],
    )
    def test_inferring_payload_format(self, raw: str, expected: str) -> None:
        assert payload_mod.infer_payload_format(raw) == expected


class TestMaterializeFilePayload:
    def test_missing_file_sources_raise_file_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match='File not found: '):
            payload_mod.materialize_file_payload(
                str(tmp_path / 'missing.json'),
                format_hint=None,
                format_explicit=False,
            )

    def test_parses_inline_json_when_hint_is_explicit(self) -> None:
        assert payload_mod.materialize_file_payload(
            '{"inline": true}',
            format_hint='json',
            format_explicit=True,
        ) == {'inline': True}

    def test_reads_existing_structured_sources_via_file(
        self,
        tmp_path: Path,
    ) -> None:
        file_path = tmp_path / 'payload.json'
        file_path.write_text('{"ok": true}', encoding='utf-8')

        payload = payload_mod.materialize_file_payload(
            str(file_path),
            format_hint=None,
            format_explicit=False,
        )

        assert payload == {'ok': True}


class TestParseTextPayload:
    def test_parse_json_payload_reports_decode_errors(self) -> None:
        with pytest.raises(ValueError, match='Invalid JSON payload'):
            JsonCodec.parse('{broken')

    @pytest.mark.parametrize(
        ('payload', 'fmt_hint', 'expected'),
        [
            ('{"a": 1}', None, {'a': 1}),
            ('a,b\n1,2\n', 'csv', [{'a': '1', 'b': '2'}]),
            ('payload', 'yaml', 'payload'),
        ],
    )
    def test_parsing_text_payload_variants(
        self,
        payload: str,
        fmt_hint: str | None,
        expected: object,
    ) -> None:
        assert payload_mod.parse_text_payload(payload, fmt_hint=fmt_hint) == expected

    def test_parses_csv_text_payload(self) -> None:
        assert payload_mod.parse_text_payload('a,b\n1,2\n3,4\n', fmt_hint='csv') == [
            {'a': '1', 'b': '2'},
            {'a': '3', 'b': '4'},
        ]
