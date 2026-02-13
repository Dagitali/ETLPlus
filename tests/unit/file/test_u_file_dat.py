"""
:mod:`tests.unit.file.test_u_file_dat` module.

Unit tests for :mod:`etlplus.file.dat`.

Notes
-----
- Exercises :func:`etlplus.file.dat._sniff` directly to keep tests
    deterministic across platforms/Python versions.
"""

from __future__ import annotations

import csv
from collections.abc import Callable
from pathlib import Path

import pytest

from etlplus.file import dat as mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions

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


def _patch_sniff(
    monkeypatch: pytest.MonkeyPatch,
    *,
    dialect: object,
    has_header: bool,
) -> None:
    """Patch DAT sniffing with deterministic dialect/header behavior."""
    monkeypatch.setattr(
        mod,
        '_sniff',
        lambda *_args, **_kwargs: (dialect, has_header),
    )


def _write_fixture_file(
    tmp_path: Path,
    filename: str,
    content: str,
) -> Path:
    """Create a fixture file in ``tmp_path`` and return its path."""
    path = tmp_path / filename
    path.write_text(content, encoding='utf-8')
    return path


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

    def has_header(self, sample: str) -> bool:  # noqa: ARG002
        """Simulate detecting whether *sample* has a header row."""
        if self._has_header_error is not None:
            raise self._has_header_error
        return self._has_header

    def sniff(
        self,
        sample: str,  # noqa: ARG002
        delimiters: str | None = None,  # noqa: ARG002
    ) -> csv.Dialect:
        """Simulate sniffing a CSV dialect from *sample*."""
        if self._sniff_error is not None:
            raise self._sniff_error
        return self._dialect


# SECTION: TESTS ============================================================ #


class TestDatSniff:
    """Unit tests for :func:`_sniff`."""

    # pylint: disable=protected-access

    @pytest.mark.parametrize(
        (
            'sniffer_factory',
            'sample',
            'expected_delimiter',
            'expected_header',
        ),
        [
            (
                lambda: _StubSniffer(
                    dialect=_make_dialect('\t'),
                    has_header=False,
                ),
                '1\t2\n3\t4\n',
                '\t',
                False,
            ),
            (
                lambda: _StubSniffer(
                    dialect=_make_dialect('|'),
                    has_header_error=csv.Error('boom'),
                ),
                'a|b\n1|2\n',
                '|',
                True,
            ),
            (
                lambda: _StubSniffer(sniff_error=csv.Error('boom')),
                'a,b\n1,2\n',
                ',',
                True,
            ),
        ],
        ids=[
            'no_header_preserved',
            'header_error_defaults_true',
            'sniff_error_falls_back_to_excel',
        ],
    )
    def test_sniff_behaviors(
        self,
        sniffer_factory: Callable[[], _StubSniffer],
        sample: str,
        expected_delimiter: str,
        expected_header: bool,
    ) -> None:
        """Test sniff behavior across success and fallback paths."""
        sniffer = sniffer_factory()

        dialect, has_header = mod._sniff(sample, sniffer=sniffer)

        assert dialect.delimiter == expected_delimiter
        assert has_header is expected_header


class TestDat:
    """Unit tests for :mod:`etlplus.file.dat`."""

    def test_read_accepts_custom_sniffer_via_read_options(
        self,
        tmp_path: Path,
    ) -> None:
        """Test DAT reads honoring custom sniffer and delimiters options."""
        handler = mod.DatFile()
        sniffer = _StubSniffer(
            dialect=_make_dialect('|'),
            has_header=False,
        )
        path = _write_fixture_file(
            tmp_path,
            'custom_sniffer.dat',
            '1|alice\n2|bob\n',
        )

        result = handler.read(
            path,
            options=ReadOptions(
                extras={'sniffer': sniffer, 'delimiters': '|'},
            ),
        )

        assert result == [
            {'col_1': '1', 'col_2': 'alice'},
            {'col_1': '2', 'col_2': 'bob'},
        ]

    def test_read_empty_returns_empty_list(
        self,
        tmp_path: Path,
    ) -> None:
        """Test reading empty input returning an empty list."""
        path = _write_fixture_file(tmp_path, 'empty.dat', '')

        assert not mod.DatFile().read(path)

    @pytest.mark.parametrize(
        ('filename', 'content', 'has_header', 'expected'),
        [
            (
                'no_header.dat',
                '1,alice\n2,bob\n',
                False,
                [
                    {'col_1': '1', 'col_2': 'alice'},
                    {'col_1': '2', 'col_2': 'bob'},
                ],
            ),
            (
                'data.dat',
                'a,b\n\n , \n1,2\n',
                True,
                [{'a': '1', 'b': '2'}],
            ),
        ],
        ids=['no_header_generates_col_names', 'blank_rows_skipped'],
    )
    def test_read_header_and_blank_row_behavior(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        filename: str,
        content: str,
        has_header: bool,
        expected: list[dict[str, str]],
    ) -> None:
        """Test no-header fallback and blank-row filtering behavior."""
        _patch_sniff(
            monkeypatch,
            dialect=csv.get_dialect('excel'),
            has_header=has_header,
        )
        path = _write_fixture_file(tmp_path, filename, content)

        assert mod.DatFile().read(path) == expected

    def test_read_ragged_rows_fill_missing_with_none_and_ignore_extras(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that rows shorter than the header are padded with ``None`` and
        extra fields beyond the header are ignored.
        """
        path = _write_fixture_file(
            tmp_path,
            'ragged.dat',
            'a,b,c\n1,2\n3,4,5,6\n',
        )

        assert mod.DatFile().read(path) == [
            {'a': '1', 'b': '2', 'c': None},
            {'a': '3', 'b': '4', 'c': '5'},
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
        _patch_sniff(
            monkeypatch,
            dialect=_make_dialect(delimiter),
            has_header=True,
        )

        path = _write_fixture_file(tmp_path, filename, content)

        assert mod.DatFile().read(path) == [{'a': '1', 'b': '2'}]

    def test_write_roundtrip_returns_written_count(
        self,
        tmp_path: Path,
    ) -> None:
        """Test write/read round trip, preserving the written row count."""
        sample_records: list[dict[str, object]] = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
        ]
        path = tmp_path / 'out.dat'

        written = mod.DatFile().write(path, sample_records)
        result = mod.DatFile().read(path)

        assert written == len(sample_records)
        assert len(result) == len(sample_records)

    def test_write_uses_delimiter_override_from_write_options(
        self,
        tmp_path: Path,
    ) -> None:
        """Test DAT writes honoring delimiter overrides from options extras."""
        handler = mod.DatFile()
        path = tmp_path / 'delimiter_override.dat'

        written = handler.write(
            path,
            [{'id': '1', 'name': 'Ada'}],
            options=WriteOptions(extras={'delimiter': '|'}),
        )

        assert written == 1
        lines = path.read_text(encoding='utf-8').splitlines()
        assert lines[0] == 'id|name'
        assert lines[1] == '1|Ada'
