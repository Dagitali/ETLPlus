"""
:mod:`tests.unit.file.test_u_file_dat` module.

Unit tests for :mod:`etlplus.file.dat`.

Notes
-----
- Focuses on branch coverage in :func:`etlplus.file.dat.read`.
- Exercises :func:`etlplus.file.dat._sniff` directly to keep tests
    deterministic across platforms/Python versions.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from etlplus.file import dat as mod

# SECTION: HELPERS ========================================================== #


def _make_dialect(delimiter: str) -> csv.Dialect:
    """Return a minimal dialect using *delimiter*."""

    dialect_type = type(
        '_TestDialect',
        (csv.Dialect,),
        {
            'delimiter': delimiter,
            'quotechar': '"',
            'escapechar': None,
            'doublequote': True,
            'skipinitialspace': False,
            'lineterminator': '\n',
            'quoting': csv.QUOTE_MINIMAL,
        },
    )
    return dialect_type()


class _StubSniffer:
    """
    Simple sniffer stub for deterministic :func:`etlplus.file.dat._sniff`
    tests.
    """

    # pylint: disable=unused-argument

    def __init__(
        self,
        *,
        dialect: csv.Dialect | None = None,
        sniff_error: Exception | None = None,
        has_header: bool = True,
        has_header_error: Exception | None = None,
    ) -> None:
        self._dialect = dialect or _make_dialect(',')
        self._sniff_error = sniff_error
        self._has_header = has_header
        self._has_header_error = has_header_error

    def sniff(
        self,
        sample: str,  # noqa: ARG002
        delimiters: str | None = None,  # noqa: ARG002
    ) -> csv.Dialect:
        """Simulate sniffing a CSV dialect from *sample*."""
        if self._sniff_error is not None:
            raise self._sniff_error
        return self._dialect

    def has_header(self, sample: str) -> bool:  # noqa: ARG002
        """Simulate detecting whether *sample* has a header row."""
        if self._has_header_error is not None:
            raise self._has_header_error
        return self._has_header


# SECTION: TESTS ============================================================ #


class TestDatSniff:
    """Unit tests for :func:`_sniff`."""

    # pylint: disable=protected-access

    def test_sniff_can_report_no_header(self) -> None:
        """
        The helper should preserve sniffer results when header detection
        succeeds.
        """
        sniffer = _StubSniffer(dialect=_make_dialect('\t'), has_header=False)

        dialect, has_header = mod._sniff('1\t2\n3\t4\n', sniffer=sniffer)

        assert dialect.delimiter == '\t'
        assert has_header is False

    def test_sniff_defaults_header_true_on_error(self) -> None:
        """
        Header detection errors should default to treating the first row as a
        header.
        """
        sniffer = _StubSniffer(
            dialect=_make_dialect('|'),
            has_header_error=csv.Error('boom'),
        )

        dialect, has_header = mod._sniff('a|b\n1|2\n', sniffer=sniffer)

        assert dialect.delimiter == '|'
        assert has_header is True

    def test_sniff_uses_excel_dialect_on_error(self) -> None:
        """Dialect sniffing errors should fall back to the Excel dialect."""

        sniffer = _StubSniffer(sniff_error=csv.Error('boom'))

        dialect, has_header = mod._sniff('a,b\n1,2\n', sniffer=sniffer)

        assert dialect.delimiter == ','
        assert has_header is True


class TestDatRead:
    """Unit tests for :func:`etlplus.file.dat.read`."""

    def test_read_empty_returns_empty_list(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that empty files yield an empty list."""
        path = tmp_path / 'empty.dat'
        path.write_text('', encoding='utf-8')

        assert not mod.read(path)

    def test_read_skips_blank_rows(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that blank rows (or rows with only whitespace) are ignored."""
        monkeypatch.setattr(
            mod,
            '_sniff',
            lambda *_args, **_kwargs: (csv.get_dialect('excel'), True),
        )
        path = tmp_path / 'data.dat'
        path.write_text(
            'a,b\n\n , \n1,2\n',
            encoding='utf-8',
        )

        assert mod.read(path) == [{'a': '1', 'b': '2'}]

    def test_read_ragged_rows_fill_missing_with_none_and_ignore_extras(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that rows shorter than the header are padded with ``None`` and
        extra fields beyond the header are ignored.
        """
        path = tmp_path / 'ragged.dat'
        path.write_text(
            'a,b,c\n1,2\n3,4,5,6\n',
            encoding='utf-8',
        )

        assert mod.read(path) == [
            {'a': '1', 'b': '2', 'c': None},
            {'a': '3', 'b': '4', 'c': '5'},
        ]

    def test_read_no_header_generates_col_names(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that when the sniffer reports no header, column names are
        generated.
        """
        monkeypatch.setattr(
            mod,
            '_sniff',
            lambda *_args, **_kwargs: (csv.get_dialect('excel'), False),
        )

        path = tmp_path / 'no_header.dat'
        path.write_text(
            '1,alice\n2,bob\n',
            encoding='utf-8',
        )

        assert mod.read(path) == [
            {'col_1': '1', 'col_2': 'alice'},
            {'col_1': '2', 'col_2': 'bob'},
        ]

    @pytest.mark.parametrize(
        ('filename', 'content', 'delimiter'),
        [
            ('tab.dat', 'a\tb\n1\t2\n', '\t'),
            ('pipe.dat', 'a|b\n1|2\n', '|'),
            ('semi.dat', 'a;b\n1;2\n', ';'),
        ],
        ids=['tab', 'pipe', 'semicolon'],
    )
    def test_read_sniffs_common_delimiters(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        filename: str,
        content: str,
        delimiter: str,
    ) -> None:
        """
        Test that DAT supports common delimiters when present (tab, pipe,
        semicolon).
        """
        dialect = _make_dialect(delimiter)
        monkeypatch.setattr(
            mod,
            '_sniff',
            lambda *_args, **_kwargs: (dialect, True),
        )

        path = tmp_path / filename
        path.write_text(content, encoding='utf-8')

        assert mod.read(path) == [{'a': '1', 'b': '2'}]


class TestDatWrite:
    """Unit tests for :func:`etlplus.file.dat.write`."""

    def test_write_round_trip(
        self,
        tmp_path: Path,
        sample_records: list[dict[str, object]],
    ) -> None:
        """
        Test that writing then reading preserves record count and produces
        non-empty output.
        """
        path = tmp_path / 'out.dat'

        written = mod.write(path, sample_records)

        assert written == len(sample_records)
        assert mod.read(path)
