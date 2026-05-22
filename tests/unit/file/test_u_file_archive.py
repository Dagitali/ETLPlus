"""
:mod:`tests.unit.file.test_u_file_archive` module.

Unit tests for :mod:`etlplus.file._archive`.
"""

from __future__ import annotations

import pytest

from etlplus.file import _archive as mod
from etlplus.file._enums import CompressionFormat
from etlplus.file._enums import FileFormat

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestArchiveHelpers:
    """Unit tests for archive payload format inference helpers."""

    @pytest.mark.parametrize(
        ('path', 'require_format', 'expected'),
        [
            pytest.param('data.unknown.gz', False, None, id='optional-unknown'),
            pytest.param('data.csv.gz', True, FileFormat.CSV, id='known-required'),
        ],
    )
    def test_infer_archive_payload_format_success_cases(
        self,
        path: str,
        require_format: bool,
        expected: FileFormat | None,
    ) -> None:
        """Test successful archive payload format inference variants."""
        assert (
            mod.infer_archive_payload_format(
                path,
                allowed_compressions=(CompressionFormat.GZ,),
                compression_error='bad compression',
                require_format=require_format,
            )
            is expected
        )

    @pytest.mark.parametrize(
        (
            'path',
            'allowed_compressions',
            'compression_error',
            'require_format',
            'match',
        ),
        [
            pytest.param(
                'data.csv.gz',
                (CompressionFormat.ZIP,),
                'compression not allowed',
                False,
                'compression not allowed',
                id='disallowed-compression',
            ),
            pytest.param(
                'data.unknown.gz',
                (CompressionFormat.GZ,),
                'bad compression',
                True,
                'Cannot infer file format',
                id='required-payload-format',
            ),
        ],
    )
    def test_infer_archive_payload_format_error_cases(
        self,
        path: str,
        allowed_compressions: tuple[CompressionFormat, ...],
        compression_error: str,
        require_format: bool,
        match: str,
    ) -> None:
        """Test archive payload format inference error variants."""
        with pytest.raises(ValueError, match=match):
            mod.infer_archive_payload_format(
                path,
                allowed_compressions=allowed_compressions,
                compression_error=compression_error,
                require_format=require_format,
            )
