"""
:mod:`tests.unit.file.test_u_file_enums` module.

Unit tests for :mod:`etlplus.file.enums`.
"""

from __future__ import annotations

import pytest

from etlplus.file import CompressionFormat
from etlplus.file import FileFormat
from etlplus.file import infer_file_format_and_compression

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


type AliasInferCase = tuple[
    str,
    FileFormat | None,
    CompressionFormat | None,
]

type InferCase = tuple[
    object,
    object | None,
    FileFormat | None,
    CompressionFormat | None,
]


def build_format_infer_result(
    fmt: FileFormat,
) -> tuple[FileFormat | None, CompressionFormat | None]:
    """Build the expected inference result for a file format input."""
    if fmt in {FileFormat.GZ, FileFormat.ZIP}:
        return None, CompressionFormat.coerce(fmt.value)
    return fmt, None


def build_enum_infer_case(
    value: FileFormat | CompressionFormat,
) -> InferCase:
    """Build the expected inference result for an enum input."""
    if isinstance(value, FileFormat):
        expected_format, expected_compression = build_format_infer_result(value)
        return value, None, expected_format, expected_compression
    return value, None, None, value


def build_file_format_alias_infer_case(
    alias: str,
    canonical: str,
) -> InferCase:
    """Build the expected inference result for a file-format alias."""
    fmt = FileFormat(canonical)
    expected_format, expected_compression = build_format_infer_result(fmt)
    value: object = (
        alias
        if '/' in alias or not alias.startswith('.')
        else f'payload{alias}'
    )
    return value, None, expected_format, expected_compression


def assert_infer_case(case: InferCase) -> None:
    """Assert that an inference case matches the implementation."""
    value, filename, expected_format, expected_compression = case
    fmt, compression = infer_file_format_and_compression(value, filename)
    assert fmt is expected_format
    assert compression is expected_compression


COMPRESSION_ENUM_INFER_CASES: tuple[InferCase, ...] = tuple(
    build_enum_infer_case(compression)
    for compression in CompressionFormat
)

COMPRESSION_ALIAS_INFER_CASES: tuple[InferCase, ...] = tuple(
    (alias, None, None, CompressionFormat(canonical))
    for alias, canonical in CompressionFormat.aliases().items()
)

EDGE_CASE_INFER_CASES: tuple[InferCase, ...] = (
    ('payload.csv.gz', None, FileFormat.CSV, CompressionFormat.GZ),
    ('payload.zip', None, None, CompressionFormat.ZIP),
    ('application/json; charset=utf-8', None, FileFormat.JSON, None),
    (
        'application/octet-stream',
        'payload.csv.gz',
        FileFormat.CSV,
        CompressionFormat.GZ,
    ),
    ('application/octet-stream', None, None, None),
    ('   ', None, None, None),
)

FILE_FORMAT_COERCE_CASES: tuple[tuple[str, FileFormat], ...] = (
    tuple((fmt.value, fmt) for fmt in FileFormat)
    + tuple(
        (alias, FileFormat(canonical))
        for alias, canonical in FileFormat.aliases().items()
    )
)

FILE_FORMAT_ENUM_INFER_CASES: tuple[InferCase, ...] = tuple(
    build_enum_infer_case(fmt)
    for fmt in FileFormat
)

FILE_FORMAT_ALIAS_INFER_CASES: tuple[InferCase, ...] = tuple(
    build_file_format_alias_infer_case(alias, canonical)
    for alias, canonical in FileFormat.aliases().items()
)


# SECTION: TESTS ============================================================ #


class TestFileFormat:
    """Unit tests for :class:`etlplus.utils.enums.FileFormat`."""

    @pytest.mark.parametrize(
        ('value', 'expected'),
        FILE_FORMAT_COERCE_CASES,
    )
    def test_coerce(
        self,
        value: str,
        expected: FileFormat,
    ) -> None:
        """Test that :meth:`coerce` resolves canonical values and aliases."""
        assert FileFormat.coerce(value) is expected

    def test_invalid_value(self) -> None:
        """
        Test that :meth:`coerce` raises :class:`ValueError` for invalid values.
        """
        with pytest.raises(ValueError, match='Invalid FileFormat'):
            FileFormat.coerce('badformat')


class TestInferFileFormatAndCompression:
    """Unit tests for :func:`infer_file_format_and_compression`."""

    @pytest.mark.parametrize(
        ('value', 'filename', 'expected_format', 'expected_compression'),
        EDGE_CASE_INFER_CASES,
    )
    def test_infers_edge_cases(
        self,
        value: object,
        filename: object | None,
        expected_format: FileFormat | None,
        expected_compression: CompressionFormat | None,
    ) -> None:
        """
        Test that :func:`infer_file_format_and_compression` handles mixed
        inputs correctly.
        """
        assert_infer_case((value, filename, expected_format, expected_compression))

    @pytest.mark.parametrize(
        ('value', 'filename', 'expected_format', 'expected_compression'),
        COMPRESSION_ALIAS_INFER_CASES,
    )
    def test_infers_for_each_compression_alias(
        self,
        value: object,
        filename: object | None,
        expected_format: FileFormat | None,
        expected_compression: CompressionFormat | None,
    ) -> None:
        """Test that every compression alias infers the expected result."""
        assert_infer_case((value, filename, expected_format, expected_compression))

    @pytest.mark.parametrize(
        ('value', 'filename', 'expected_format', 'expected_compression'),
        COMPRESSION_ENUM_INFER_CASES,
    )
    def test_infers_for_each_compression_enum(
        self,
        value: object,
        filename: object | None,
        expected_format: FileFormat | None,
        expected_compression: CompressionFormat | None,
    ) -> None:
        """Test that every compression enum infers the expected result."""
        assert_infer_case((value, filename, expected_format, expected_compression))

    @pytest.mark.parametrize(
        ('value', 'filename', 'expected_format', 'expected_compression'),
        FILE_FORMAT_ALIAS_INFER_CASES,
    )
    def test_infers_for_each_file_format_alias(
        self,
        value: object,
        filename: object | None,
        expected_format: FileFormat | None,
        expected_compression: CompressionFormat | None,
    ) -> None:
        """Test that every file-format alias infers the expected result."""
        assert_infer_case((value, filename, expected_format, expected_compression))

    @pytest.mark.parametrize(
        ('value', 'filename', 'expected_format', 'expected_compression'),
        FILE_FORMAT_ENUM_INFER_CASES,
    )
    def test_infers_for_each_file_format_enum(
        self,
        value: object,
        filename: object | None,
        expected_format: FileFormat | None,
        expected_compression: CompressionFormat | None,
    ) -> None:
        """Test that every file format enum infers the expected result."""
        assert_infer_case((value, filename, expected_format, expected_compression))
