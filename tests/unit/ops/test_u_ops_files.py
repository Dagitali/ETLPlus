"""
:mod:`tests.unit.ops.test_u_ops_files` module.

Unit tests for :mod:`etlplus.ops._files`.
"""

from __future__ import annotations

import importlib
from pathlib import Path

from etlplus.file import FileFormat

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


files_mod = importlib.import_module('etlplus.ops._files')


class CapturingFile:
    """File double that records construction arguments."""

    file_format = FileFormat.CSV
    captured: dict[str, object] = {}

    def __init__(self, path: object, file_format: object = None) -> None:
        self.__class__.captured = {
            'path': path,
            'file_format': file_format,
        }


# SECTION: TESTS ============================================================ #


class TestResolveFile:
    """Unit tests for shared file resolution helpers."""

    def test_infers_format_without_default(self, tmp_path: Path) -> None:
        """Inference should preserve ``None`` when no format is detected."""
        resolved = files_mod.resolve_file(tmp_path / 'payload', None)

        assert resolved.file.path == tmp_path / 'payload'
        assert resolved.file_format is None

    def test_infers_format_with_default_fallback(
        self,
        tmp_path: Path,
    ) -> None:
        """Inference should fall back to the supplied default format."""
        resolved = files_mod.resolve_file(
            tmp_path / 'payload',
            None,
            inferred_default=FileFormat.JSON,
        )

        assert resolved.file.path == tmp_path / 'payload'
        assert resolved.file_format is FileFormat.JSON

    def test_uses_explicit_format_and_custom_file_class(self) -> None:
        """Explicit formats should be coerced and passed to the file class."""
        CapturingFile.captured = {}

        resolved = files_mod.resolve_file(
            's3://bucket/data.csv',
            'csv',
            file_cls=CapturingFile,
        )

        assert CapturingFile.captured == {
            'path': 's3://bucket/data.csv',
            'file_format': FileFormat.CSV,
        }
        assert resolved.file_format is FileFormat.CSV
