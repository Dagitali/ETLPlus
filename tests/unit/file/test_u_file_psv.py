"""
:mod:`tests.unit.file.test_u_file_psv` module.

Unit tests for :mod:`etlplus.file.psv`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import psv as mod

# SECTION: TESTS ============================================================ #


class TestPsvDelegation:
    """Unit tests for PSV read/write delegation."""

    def test_read_uses_pipe_delimiter(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`read` uses the pipe delimiter when delegating to
        :func:`read_delimited`.
        """
        calls: dict[str, object] = {}

        def _read_delimited(
            path: object, *, delimiter: str,
        ) -> list[dict[str, object]]:
            calls['path'] = path
            calls['delimiter'] = delimiter
            return [{'ok': True}]

        monkeypatch.setattr(mod, 'read_delimited', _read_delimited)

        result = mod.read(tmp_path / 'data.psv')

        assert result == [{'ok': True}]
        assert calls['delimiter'] == '|'

    def test_write_uses_pipe_delimiter(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`write` uses the pipe delimiter when delegating to
        :func:`write_delimited`.
        """
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

        written = mod.write(tmp_path / 'data.psv', [{'id': 1}])

        assert written == 1
        assert calls['delimiter'] == '|'
        assert calls['format_name'] == 'PSV'
