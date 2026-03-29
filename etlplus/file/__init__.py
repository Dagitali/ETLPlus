"""
:mod:`etlplus.file` package.

Public file IO helpers.
"""

from __future__ import annotations

from ._core import File
from ._enums import CompressionFormat
from ._enums import FileFormat
from ._enums import infer_file_format_and_compression

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Class
    'File',
    # Enums
    'CompressionFormat',
    'FileFormat',
    # Functions
    'infer_file_format_and_compression',
]
