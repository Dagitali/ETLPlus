"""
:mod:`tests.unit.file.test_u_file_tab` module.

Unit tests for :mod:`etlplus.file.tab`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import tab as mod

# SECTION: TESTS ============================================================ #


class TestTabRead:
    """Unit tests for :func:`etlplus.file.tab.read`."""

    def test_read_uses_tab_delimiter(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that reading uses a tab delimiter."""
        calls: dict[str, object] = {}

        def _read_delimited(
            path: object,
            *,
            delimiter: str,
        ) -> list[dict[str, object]]:
            calls['path'] = path
            calls['delimiter'] = delimiter
            return [{'ok': True}]

        monkeypatch.setattr(mod, 'read_delimited', _read_delimited)

        result = mod.read(tmp_path / 'data.tab')

        assert result == [{'ok': True}]
        assert calls['delimiter'] == '\t'


class TestTabWrite:
    """Unit tests for :func:`etlplus.file.tab.write`."""

    def test_write_uses_tab_delimiter(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that writing uses a tab delimiter."""
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

        written = mod.write(tmp_path / 'data.tab', [{'id': 1}])

        assert written == 1
        assert calls['delimiter'] == '\t'
        assert calls['format_name'] == 'TAB'
