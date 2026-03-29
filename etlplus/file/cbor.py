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

from ._binary_codec_handlers import BinaryRecordCodecHandlerMixin
from ._enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'CborFile',
]

# SECTION: CLASSES ========================================================== #


class CborFile(BinaryRecordCodecHandlerMixin):
    """Handler implementation for CBOR files."""

    # -- Class Attributes -- #

    format = FileFormat.CBOR
    codec_module_name = 'cbor2'
    codec_format_name = 'CBOR'
    dependency_required = True
    encode_method_name = 'dumps'
    decode_method_name = 'loads'
