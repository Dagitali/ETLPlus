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

# SECTION: TESTS ============================================================ #


class TestNdjsonRead:
    """Unit tests for :func:`etlplus.file.ndjson.read`."""

    def test_read_skips_blank_lines(
        self,
        tmp_path: Path,
    ) -> None:
        """T
        est that :func:`read` skips blank lines and lines with only
        whitespace.
        """
        path = tmp_path / 'data.ndjson'
        path.write_text(
            '{"id": 1}\n\n   \n{"id": 2}\n',
            encoding='utf-8',
        )

        assert mod.read(path) == [{'id': 1}, {'id': 2}]

    def test_read_rejects_non_dict_line(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`read` rejects lines that do not contain JSON objects.
        """
        path = tmp_path / 'data.ndjson'
        path.write_text(
            '{"id": 1}\n42\n',
            encoding='utf-8',
        )

        with pytest.raises(TypeError, match='line 2'):
            mod.read(path)

    def test_read_raises_for_invalid_json(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`read` raises a JSONDecodeError for lines that do not
        contain valid JSON.
        """
        path = tmp_path / 'data.ndjson'
        path.write_text('{"id": 1}\n{broken\n', encoding='utf-8')

        with pytest.raises(json.JSONDecodeError):
            mod.read(path)


class TestNdjsonWrite:
    """Unit tests for :func:`etlplus.file.ndjson.write`."""

    def test_write_empty_returns_zero(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that writing an empty payload returns zero and creates no file.
        """
        path = tmp_path / 'data.ndjson'

        assert mod.write(path, []) == 0
        assert not path.exists()

    def test_write_writes_each_record_on_its_own_line(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that writing writes each record on its own line.
        """
        path = tmp_path / 'data.ndjson'
        payload = [{'id': 1}, {'id': 2}]

        written = mod.write(path, payload)

        assert written == 2
        lines = path.read_text(encoding='utf-8').splitlines()
        assert [json.loads(line) for line in lines] == payload

    def test_write_accepts_single_dict(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing accepts a single dictionary as payload."""
        path = tmp_path / 'data.ndjson'

        written = mod.write(path, {'id': 1})

        assert written == 1
        assert path.read_text(encoding='utf-8').strip() == '{"id": 1}'

    def test_write_rejects_non_dict_records(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that writing rejects records that are not dictionaries."""
        path = tmp_path / 'data.ndjson'

        with pytest.raises(TypeError, match='NDJSON payloads must contain'):
            mod.write(path, cast(list[dict[str, Any]], [1]))
