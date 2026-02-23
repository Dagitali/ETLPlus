"""
:mod:`etlplus.file.cbor` module.

Helpers for reading/writing Concise Binary Object Representation (CBOR) files.

Notes
-----
- A CBOR file is a binary data format designed for small code size and message
    size, suitable for constrained environments.
- Common cases:
    - IoT data interchange.
    - Efficient data serialization.
    - Storage of structured data in a compact binary form.
- Rule of thumb:
    - If the file follows the CBOR specification, use this module for reading
        and writing.
"""

from __future__ import annotations

from typing import Any

from ._binary_codec_handlers import BinaryRecordCodecHandlerMixin
from ._imports import get_dependency
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'CborFile',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _cbor2() -> Any:
    """Return the optional cbor2 module."""
    return get_dependency('cbor2', format_name='CBOR')


# SECTION: CLASSES ========================================================== #


class CborFile(BinaryRecordCodecHandlerMixin):
    """
    Handler implementation for CBOR files.
    """

    # -- Class Attributes -- #

    format = FileFormat.CBOR
    codec_module_name = 'cbor2'
    codec_format_name = 'CBOR'
    encode_method_name = 'dumps'
    decode_method_name = 'loads'

    # -- Internal Instance Methods -- #

    def resolve_codec_module(self) -> Any:
        """
        Return the optional cbor2 module.
        """
        return _cbor2()
