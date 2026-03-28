"""
:mod:`tests.unit.file.test_u_file_init` module.

Unit tests for :mod:`etlplus.file.__init__`.
"""

from __future__ import annotations

from etlplus import file as mod
from etlplus.file._core import File
from etlplus.file._enums import CompressionFormat
from etlplus.file._enums import FileFormat
from etlplus.file._enums import infer_file_format_and_compression

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestFilePackageExports:
    """Unit tests for public package exports."""

    def test_all_exports_are_expected_and_importable(self) -> None:
        """Test that ``__all__`` and top-level package symbol wiring."""
        assert mod.__all__ == [
            'File',
            'CompressionFormat',
            'FileFormat',
            'infer_file_format_and_compression',
        ]
        assert mod.File is File
        assert mod.CompressionFormat is CompressionFormat
        assert mod.FileFormat is FileFormat
        assert mod.infer_file_format_and_compression is (
            infer_file_format_and_compression
        )
