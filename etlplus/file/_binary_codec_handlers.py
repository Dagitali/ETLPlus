"""
:mod:`etlplus.file._binary_codec_handlers` module.

Shared abstractions for binary record codec handlers.
"""

from __future__ import annotations

from typing import Any
from typing import ClassVar

from ..utils.types import JSONData
from ._imports import get_dependency
from ._io import coerce_record_payload
from ._io import normalize_records
from .base import BinarySerializationFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'BinaryRecordCodecHandlerMixin',
]


# SECTION: CLASSES ========================================================== #


class BinaryRecordCodecHandlerMixin(BinarySerializationFileHandlerABC):
    """
    Shared implementation for binary codecs that encode/decode record payloads.
    """

    codec_module_name: ClassVar[str]
    codec_format_name: ClassVar[str]
    codec_pip_name: ClassVar[str | None] = None
    dependency_required: ClassVar[bool] = False
    encode_method_name: ClassVar[str]
    decode_method_name: ClassVar[str]
    encode_kwargs: ClassVar[tuple[tuple[str, object], ...]] = ()
    decode_kwargs: ClassVar[tuple[tuple[str, object], ...]] = ()

    # -- Instance Methods -- #

    def decode_payload(
        self,
        codec_module: Any,
        payload: bytes,
    ) -> object:
        """
        Decode payload bytes using codec-specific reader settings.
        """
        decoder = getattr(codec_module, self.decode_method_name)
        return decoder(payload, **dict(self.decode_kwargs))

    def dumps_bytes(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> bytes:
        """
        Serialize structured records to codec-specific bytes.
        """
        _ = options
        codec = self.resolve_codec_module()
        records = normalize_records(data, self.format_name)
        payload: JSONData = records if isinstance(data, list) else records[0]
        return self.encode_payload(codec, payload)

    def encode_payload(
        self,
        codec_module: Any,
        payload: JSONData,
    ) -> bytes:
        """
        Encode a payload using codec-specific writer settings.
        """
        encoder = getattr(codec_module, self.encode_method_name)
        return encoder(payload, **dict(self.encode_kwargs))

    def loads_bytes(
        self,
        payload: bytes,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse codec bytes into record payloads.
        """
        _ = options
        codec = self.resolve_codec_module()
        decoded = self.decode_payload(codec, payload)
        return coerce_record_payload(decoded, format_name=self.format_name)

    def resolve_codec_module(self) -> Any:
        """
        Return the codec module for this handler.
        """
        return get_dependency(
            self.codec_module_name,
            format_name=self.codec_format_name,
            pip_name=self.codec_pip_name,
            required=self.dependency_required,
        )
