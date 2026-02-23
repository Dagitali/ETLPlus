"""
:mod:`etlplus.file.toml` module.

Helpers for reading/writing Tom's Obvious Minimal Language (TOML) files.

Notes
-----
- A TOML file is a configuration file that uses the TOML syntax.
- Common cases:
    - Simple key-value pairs.
    - Nested tables and arrays.
    - Data types such as strings, integers, floats, booleans, dates, and
        arrays.
- Rule of thumb:
    - If the file follows the TOML specification, use this module for
        reading and writing.
"""

from __future__ import annotations

import tomllib
from typing import Any

from ..types import JSONData
from ..types import JSONDict
from ._imports import get_optional_module
from ._io import make_deprecated_module_io
from .base import DictPayloadSemiStructuredTextFileHandlerABC
from .base import ReadOptions
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'TomlFile',
    # Functions
    'read',
    'write',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _tomli_w() -> Any:
    """Return the optional tomli_w module for TOML writes."""
    return get_optional_module(
        'tomli_w',
        error_message=(
            'TOML write support requires optional dependency '
            '"tomli_w".\n'
            'Install with: pip install tomli-w'
        ),
    )


def _toml() -> Any:
    """Return the optional toml module as TOML write fallback."""
    return get_optional_module(
        'toml',
        error_message=(
            'TOML write support requires optional dependency '
            '"tomli_w" or "toml".\n'
            'Install with: pip install tomli-w'
        ),
    )


# SECTION: CLASSES ========================================================== #


class TomlFile(DictPayloadSemiStructuredTextFileHandlerABC):
    """
    Handler implementation for TOML files.
    """

    # -- Class Attributes -- #

    format = FileFormat.TOML

    # -- Instance Methods -- #

    def dumps_dict_payload(
        self,
        payload: JSONDict,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize dictionary *data* into TOML text.

        Parameters
        ----------
        payload : JSONDict
            Dictionary payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            Serialized TOML text.
        """
        _ = options

        toml_writer: Any
        try:
            toml_writer = _tomli_w()
            return str(toml_writer.dumps(payload))
        except ImportError:
            toml = _toml()
            return str(toml.dumps(payload))

    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse TOML *text* into a dictionary payload.

        Parameters
        ----------
        text : str
            TOML payload as text.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed payload.

        Raises
        ------
        TypeError
            If the TOML root is not a table (dictionary).
        """
        _ = options
        return self.coerce_dict_root_payload(
            tomllib.loads(text),
            error_message='TOML root must be a table (dict)',
        )


# SECTION: INTERNAL CONSTANTS =============================================== #

_TOML_HANDLER = TomlFile()


# SECTION: FUNCTIONS ======================================================== #


read, write = make_deprecated_module_io(__name__, _TOML_HANDLER)
