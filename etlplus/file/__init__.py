"""
:mod:`etlplus.file` package.

Public file IO helpers.
"""

from __future__ import annotations

from .core import File
from .enums import CompressionFormat
from .enums import FileFormat
from .enums import infer_file_format_and_compression

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
