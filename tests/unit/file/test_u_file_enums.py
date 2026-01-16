"""
:mod:`tests.unit.test_u_file_enums` module.

Unit tests for :mod:`etlplus.file.enums`.

Notes
-----
- Exercises format/compression inference rules and coercion helpers.
"""

from __future__ import annotations

import pytest

from etlplus.file import CompressionFormat
from etlplus.file import FileFormat
from etlplus.file import infer_file_format_and_compression

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


type InferCase = tuple[
    object,
    object | None,
    FileFormat | None,
    CompressionFormat | None,
]

INFER_CASES: list[InferCase] = [
    ('data.csv.gz', None, FileFormat.CSV, CompressionFormat.GZ),
    ('data.jsonl.gz', None, FileFormat.NDJSON, CompressionFormat.GZ),
    ('data.zip', None, None, CompressionFormat.ZIP),
    ('application/json; charset=utf-8', None, FileFormat.JSON, None),
    ('application/gzip', None, None, CompressionFormat.GZ),
    (
        'application/octet-stream',
        'payload.csv.gz',
        FileFormat.CSV,
        CompressionFormat.GZ,
    ),
    ('application/octet-stream', None, None, None),
    (FileFormat.GZ, None, None, CompressionFormat.GZ),
    (CompressionFormat.ZIP, None, None, CompressionFormat.ZIP),
]


# SECTION: TESTS ============================================================ #


class TestFileFormat:
    """Unit test suite for :class:`etlplus.enums.FileFormat`."""

    @pytest.mark.parametrize(
        'value,expected',
        [
            ('JSON', FileFormat.JSON),
            ('application/xml', FileFormat.XML),
            ('yml', FileFormat.YAML),
        ],
    )
    def test_aliases(
        self,
        value: str,
        expected: FileFormat,
    ) -> None:
        """Test alias coercions."""
        assert FileFormat.coerce(value) is expected

    def test_coerce(self) -> None:
        """Test :meth:`coerce`."""
        assert FileFormat.coerce('csv') is FileFormat.CSV

    def test_invalid_value(self) -> None:
        """Test that invalid values raise ValueError."""
        with pytest.raises(ValueError, match='Invalid FileFormat'):
            FileFormat.coerce('badformat')


class TestInferFileFormatAndCompression:
    """Unit test suite for :func:`infer_file_format_and_compression`."""

    @pytest.mark.parametrize(
        'value,filename,expected_format,expected_compression',
        INFER_CASES,
    )
    def test_infers_format_and_compression(
        self,
        value: object,
        filename: object | None,
        expected_format: FileFormat | None,
        expected_compression: CompressionFormat | None,
    ) -> None:
        """Test mixed inputs for format and compression inference."""
        fmt, compression = infer_file_format_and_compression(value, filename)
        assert fmt is expected_format
        assert compression is expected_compression
