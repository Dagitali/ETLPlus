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

    def test_infer_archive_payload_format_rejects_disallowed_compression(
        self,
    ) -> None:
        """Test that disallowed compression raises the provided error."""
        with pytest.raises(ValueError, match='compression not allowed'):
            mod.infer_archive_payload_format(
                'data.csv.gz',
                allowed_compressions=(CompressionFormat.ZIP,),
                compression_error='compression not allowed',
            )

    def test_infer_archive_payload_format_requires_payload_format(
        self,
    ) -> None:
        """
        Test that unknown compressed files fail when payload format is
        required.
        """
        with pytest.raises(ValueError, match='Cannot infer file format'):
            mod.infer_archive_payload_format(
                'data.unknown.gz',
                allowed_compressions=(CompressionFormat.GZ,),
                compression_error='bad compression',
                require_format=True,
            )
