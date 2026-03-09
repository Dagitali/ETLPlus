"""
:mod:`tests.unit.file.test_u_file_txt` module.

Unit tests for :mod:`etlplus.file.txt`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import txt as mod

from .pytest_file_contract_mixins import RoundtripUnitModuleContract
from .pytest_file_roundtrip_spec import RoundtripSpec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestTxt(RoundtripUnitModuleContract):
    """Unit tests for :mod:`etlplus.file.txt`."""

    module = mod
    format_name = 'txt'
    roundtrip_spec = RoundtripSpec(
        payload='alpha\n\nbeta\n',
        expected='alpha\n\nbeta\n',
        expected_written_count=3,
    )

    def test_read_returns_raw_text(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`read` returns the raw TXT payload."""
        path = self.format_path(tmp_path)
        path.write_text('alpha\n\nbeta\n', encoding='utf-8')

        assert mod.TxtFile().read(path) == 'alpha\n\nbeta\n'

    def test_read_rows_returns_plain_lines(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`read_rows` returns plain lines."""
        path = self.format_path(tmp_path)
        path.write_text('alpha\n\nbeta\n', encoding='utf-8')

        assert mod.TxtFile().read_rows(path) == ['alpha', '', 'beta']

    def test_write_accepts_raw_text(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`write` preserves raw text payloads."""
        path = self.format_path(tmp_path)

        written = mod.TxtFile().write(path, 'alpha\n\nbeta\n')

        assert written == 3
        assert path.read_text(encoding='utf-8') == 'alpha\n\nbeta\n'

    def test_write_rows_accepts_plain_lines(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that :func:`write_rows` accepts a list of strings."""
        path = self.format_path(tmp_path)

        written = mod.TxtFile().write_rows(path, ['alpha', '', 'beta'])

        assert written == 3
        assert path.read_text(encoding='utf-8') == 'alpha\n\nbeta'

    def test_write_accepts_legacy_text_records(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that legacy ``{"text": ...}`` payloads remain accepted."""
        path = self.format_path(tmp_path)

        written = mod.TxtFile().write(
            path,
            [{'text': 'alpha'}, {'text': 'beta'}],
        )

        assert written == 2
        assert path.read_text(encoding='utf-8') == 'alpha\nbeta'

    @pytest.mark.parametrize(
        'payload',
        ['', []],
        ids=['empty_text', 'empty_lines'],
    )
    def test_write_empty_payload_creates_empty_file(
        self,
        tmp_path: Path,
        payload: str | list[str],
    ) -> None:
        """Test that empty TXT payloads write empty files."""
        path = self.format_path(tmp_path)

        written = mod.TxtFile().write(path, payload)

        assert written == 0
        assert path.exists()
        assert path.read_text(encoding='utf-8') == ''

    @pytest.mark.parametrize(
        'payload',
        [
            {'nope': 'value'},
            [{'text': 1}],
            [1],
            123,
        ],
        ids=[
            'mapping_without_text',
            'legacy_record_without_string_text',
            'non_string_list_item',
            'scalar',
        ],
    )
    def test_write_rejects_non_text_payloads(
        self,
        tmp_path: Path,
        payload: object,
    ) -> None:
        """Test that :func:`write` rejects non-text payloads."""
        path = self.format_path(tmp_path)

        with pytest.raises(TypeError, match='TXT payload'):
            mod.TxtFile().write(path, payload)
