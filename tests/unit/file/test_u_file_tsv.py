"""
:mod:`tests.unit.file.test_u_file_tsv` module.

Unit tests for :mod:`etlplus.file.tsv`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import tsv as mod

# SECTION: TESTS ============================================================ #


class TestTsvDelegation:
    """Unit tests for TSV read/write delegation."""

    def test_read_uses_tab_delimiter(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`read` uses a tab delimiter."""
        calls: dict[str, object] = {}

        def _read_delimited(
            path: object, *, delimiter: str,
        ) -> list[dict[str, object]]:
            calls['path'] = path
            calls['delimiter'] = delimiter
            return [{'ok': True}]

        monkeypatch.setattr(mod, 'read_delimited', _read_delimited)

        result = mod.read(tmp_path / 'data.tsv')

        assert result == [{'ok': True}]
        assert calls['delimiter'] == '\t'

    def test_write_uses_tab_delimiter(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`write` uses a tab delimiter."""
        calls: dict[str, object] = {}

        def _write_delimited(
            path: object,
            data: object,
            *,
            delimiter: str,
            format_name: str,
        ) -> int:
            calls['path'] = path
            calls['data'] = data
            calls['delimiter'] = delimiter
            calls['format_name'] = format_name
            return 1

        monkeypatch.setattr(mod, 'write_delimited', _write_delimited)

        written = mod.write(tmp_path / 'data.tsv', [{'id': 1}])

        assert written == 1
        assert calls['delimiter'] == '\t'
        assert calls['format_name'] == 'TSV'
