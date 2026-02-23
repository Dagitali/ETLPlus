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

from typing import Any

from ._binary_codec_handlers import BinaryRecordCodecHandlerMixin
from ._imports import get_dependency
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'MsgpackFile',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _msgpack() -> Any:
    """Return the optional msgpack module."""
    return get_dependency('msgpack', format_name='MSGPACK')


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

    # -- Internal Instance Methods -- #

    def resolve_codec_module(self) -> Any:
        """
        Return the optional msgpack module.
        """
        return _msgpack()
