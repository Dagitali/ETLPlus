"""
:mod:`tests.unit.file.test_u_file_init` module.

Unit tests for :mod:`etlplus.file` package facade exports.
"""

from __future__ import annotations

import pytest

from etlplus import file as file_pkg
from etlplus.file._core import File
from etlplus.file._enums import CompressionFormat
from etlplus.file._enums import FileFormat
from etlplus.file._enums import infer_file_format_and_compression
from etlplus.file.base import BoundFileHandler
from etlplus.file.base import ReadOptions
from etlplus.file.base import WriteOptions

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


FILE_EXPORTS = [
    ('BoundFileHandler', BoundFileHandler),
    ('File', File),
    ('ReadOptions', ReadOptions),
    ('WriteOptions', WriteOptions),
    ('CompressionFormat', CompressionFormat),
    ('FileFormat', FileFormat),
    ('infer_file_format_and_compression', infer_file_format_and_compression),
]

# SECTION: TESTS ============================================================ #


class TestFilePackageExports:
    """Unit tests for package-level exports."""

    def test_expected_symbols(self) -> None:
        """
        Test that package facade preserves the documented export order of the
        public API surface (i.e., ``__all__`` contract).
        """
        assert file_pkg.__all__ == [name for name, _value in FILE_EXPORTS]

    @pytest.mark.parametrize(('name', 'expected'), FILE_EXPORTS)
    def test_expected_symbol_bindings(
        self,
        name: str,
        expected: object,
    ) -> None:
        """Test that package exports resolve to their canonical objects."""
        assert getattr(file_pkg, name) == expected
