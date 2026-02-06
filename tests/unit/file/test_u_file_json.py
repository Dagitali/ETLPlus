"""
:mod:`tests.unit.file.test_u_file_json` module.

Unit tests for :mod:`etlplus.file.json`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from etlplus.file import json as mod

# SECTION: TESTS ============================================================ #


class TestJsonRead:
    """Unit tests for :func:`etlplus.file.json.read`."""

    def test_read_list_of_records(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`read` correctly reads a list of JSON objects as
        records.
        """
        path = tmp_path / 'data.json'
        path.write_text(json.dumps([{'id': 1}]), encoding='utf-8')

        assert mod.read(path) == [{'id': 1}]

    def test_read_rejects_non_object_root(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`read` rejects a JSON array as root since it cannot be
        treated as records.
        """
        path = tmp_path / 'data.json'
        path.write_text(json.dumps([1, 2]), encoding='utf-8')

        with pytest.raises(TypeError, match='JSON array must contain'):
            mod.read(path)

    def test_read_accepts_dict_root(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`read` accepts a JSON object as root."""
        path = tmp_path / 'data.json'
        payload = {'id': 1}
        path.write_text(json.dumps(payload), encoding='utf-8')

        assert mod.read(path) == payload

    def test_read_rejects_scalar_root(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`read` rejects a scalar JSON value as root."""
        path = tmp_path / 'data.json'
        path.write_text('42', encoding='utf-8')

        with pytest.raises(TypeError, match='JSON root must be'):
            mod.read(path)


class TestJsonWrite:
    """Unit tests for :func:`etlplus.file.json.write`."""

    def test_write_adds_newline_and_counts_records(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`write` adds a newline and counts records correctly.
        """
        path = tmp_path / 'data.json'
        payload = [{'id': 1}, {'id': 2}]

        written = mod.write(path, payload)

        assert written == 2
        content = path.read_text(encoding='utf-8')
        assert content.endswith('\n')

    def test_write_accepts_dict_payload(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :func:`write` accepts a dict payload and counts as 1 record.
        """
        path = tmp_path / 'data.json'
        payload = {'id': 1}

        written = mod.write(path, payload)

        assert written == 1
        content = path.read_text(encoding='utf-8')
        assert content.endswith('\n')
        assert json.loads(content) == payload
