"""
:mod:`tests.unit.file.test_u_file_archive` module.

Unit tests for :mod:`etlplus.file._archive`.
"""

from __future__ import annotations

import pytest

from etlplus.file import _archive as mod
from etlplus.file.enums import CompressionFormat
from etlplus.file.enums import FileFormat

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestArchiveHelpers:
    """Unit tests for archive payload format inference helpers."""

    def test_infer_archive_payload_format_allows_missing_payload_format(
        self,
    ) -> None:
        """Test optional payload format returning ``None`` when unresolved."""
        assert (
            mod.infer_archive_payload_format(
                'data.unknown.gz',
                allowed_compressions=(CompressionFormat.GZ,),
                compression_error='bad compression',
                require_format=False,
            )
            is None
        )

    def test_infer_archive_payload_format_happy_path(self) -> None:
        """Test inferring payload format when compression is allowed."""
        result = mod.infer_archive_payload_format(
            'data.csv.gz',
            allowed_compressions=(CompressionFormat.GZ,),
            compression_error='bad compression',
        )
        assert result is FileFormat.CSV

    def test_infer_archive_payload_format_rejects_disallowed_compression(
        self,
    ) -> None:
        """Test disallowed compression raising the provided error."""
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
        Test required payload format failure for unknown compressed files.
        """
        with pytest.raises(ValueError, match='Cannot infer file format'):
            mod.infer_archive_payload_format(
                'data.unknown.gz',
                allowed_compressions=(CompressionFormat.GZ,),
                compression_error='bad compression',
                require_format=True,
            )
