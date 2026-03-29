"""
:mod:`etlplus.file._semi_structured_handlers` module.

Shared abstractions for semi-structured text handlers.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import ClassVar

from ..utils._types import JSONData
from ..utils._types import JSONDict
from .base import DictPayloadSemiStructuredTextFileHandlerABC
from .base import ReadOptions
from .base import RecordPayloadSemiStructuredTextFileHandlerABC
from .base import WriteOptions

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'DictPayloadTextCodecHandlerMixin',
    'RecordPayloadTextCodecHandlerMixin',
]


# SECTION: CLASSES ========================================================== #


class RecordPayloadTextCodecHandlerMixin(
    RecordPayloadSemiStructuredTextFileHandlerABC,
):
    """Shared record-payload text codec flow for JSON/YAML-style handlers."""

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def encode_text_payload(
        self,
        data: JSONData,
    ) -> str:
        """
        Encode structured payload data to text.

        Parameters
        ----------
        data : JSONData
            The structured data to encode as text.

        Returns
        -------
        str
            The encoded text.
        """

    @abstractmethod
    def decode_text_payload(
        self,
        text: str,
    ) -> object:
        """
        Decode text payload to a Python object.

        Parameters
        ----------
        text : str
            The text to decode.

        Returns
        -------
        object
            The decoded Python object.
        """

    # -- Instance Methods -- #

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize structured payload data to text.

        Parameters
        ----------
        data : JSONData
            The structured data to encode as text.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            The encoded text.
        """
        _ = options
        return self.encode_text_payload(data)

    def loads_payload(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> object:
        """
        Parse raw text into a Python payload.

        Parameters
        ----------
        text : str
            The text to decode.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        object
            The decoded Python object.
        """
        _ = options
        return self.decode_text_payload(text)


class DictPayloadTextCodecHandlerMixin(
    DictPayloadSemiStructuredTextFileHandlerABC,
):
    """Shared dict-payload text codec flow for TOML/INI/PROPERTIES handlers."""

    dict_root_error_message: ClassVar[str | None] = None

    # -- Abstract Instance Methods -- #

    @abstractmethod
    def encode_dict_payload_text(
        self,
        payload: JSONDict,
    ) -> str:
        """
        Encode dictionary payload to text.

        Parameters
        ----------
        payload : JSONDict
            The dictionary payload to encode.

        Returns
        -------
        str
            The encoded text.
        """

    @abstractmethod
    def decode_dict_payload_text(
        self,
        text: str,
    ) -> object:
        """
        Decode text payload to a Python object.

        Parameters
        ----------
        text : str
            The text to decode.

        Returns
        -------
        object
            The decoded Python object.
        """

    # -- Instance Methods -- #

    def dumps_dict_payload(
        self,
        payload: JSONDict,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize one dictionary payload to text.

        Parameters
        ----------
        payload : JSONDict
            The dictionary payload to encode.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            The encoded text.
        """
        _ = options
        return self.encode_dict_payload_text(payload)

    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse text into a dictionary payload.

        Parameters
        ----------
        text : str
            The text to decode.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            The decoded dictionary payload.
        """
        _ = options
        decoded = self.decode_dict_payload_text(text)
        return self.coerce_dict_root_payload(
            decoded,
            error_message=self.dict_root_error_message,
        )
