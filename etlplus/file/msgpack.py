"""
:mod:`etlplus.file.msgpack` module.

Helpers for reading/writing MessagePack (MSGPACK) files.

Notes
-----
- A MsgPack file is a binary serialization format that is more compact than
    JSON.
- Common cases:
    - Efficient data storage and transmission.
    - Inter-process communication.
    - Data serialization in performance-critical applications.
- Rule of thumb:
    - If the file follows the MsgPack specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from ._binary_codec_handlers import BinaryRecordCodecHandlerMixin
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MsgpackFile',
]

# SECTION: CLASSES ========================================================== #


class MsgpackFile(BinaryRecordCodecHandlerMixin):
    """
    Handler implementation for MessagePack files.
    """

    # -- Class Attributes -- #

    format = FileFormat.MSGPACK
    codec_module_name = 'msgpack'
    codec_format_name = 'MSGPACK'
    encode_method_name = 'packb'
    decode_method_name = 'unpackb'
    encode_kwargs = (('use_bin_type', True),)
    decode_kwargs = (('raw', False),)
