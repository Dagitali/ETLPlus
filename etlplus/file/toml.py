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
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import StrPath
from ._imports import get_optional_module
from ._io import call_deprecated_module_read
from ._io import call_deprecated_module_write
from ._io import require_dict_payload
from .base import ReadOptions
from .base import SemiStructuredTextFileHandlerABC
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


# SECTION: CLASSES ========================================================== #


class TomlFile(SemiStructuredTextFileHandlerABC):
    """
    Handler implementation for TOML files.
    """

    # -- Class Attributes -- #

    format = FileFormat.TOML
    allow_dict_root = True
    allow_list_root = False

    # -- Instance Methods -- #

    def count_written_records(
        self,
        data: JSONData,
    ) -> int:
        """
        Return one record for dictionary-shaped TOML payload writes.
        """
        _ = data
        return 1

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize dictionary *data* into TOML text.

        Parameters
        ----------
        data : JSONData
            Payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            Serialized TOML text.
        """
        _ = options
        payload = require_dict_payload(data, format_name='TOML')

        toml_writer: Any
        try:
            toml_writer = get_optional_module(
                'tomli_w',
                error_message=(
                    'TOML write support requires optional dependency '
                    '"tomli_w".\n'
                    'Install with: pip install tomli-w'
                ),
            )
            return str(toml_writer.dumps(cast(JSONDict, payload)))
        except ImportError:
            toml = get_optional_module(
                'toml',
                error_message=(
                    'TOML write support requires optional dependency '
                    '"tomli_w" or "toml".\n'
                    'Install with: pip install tomli-w'
                ),
            )
            return str(toml.dumps(cast(JSONDict, payload)))

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
        payload = tomllib.loads(text)
        if isinstance(payload, dict):
            return payload
        raise TypeError('TOML root must be a table (dict)')


# SECTION: INTERNAL CONSTANTS =============================================== #

_TOML_HANDLER = TomlFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Deprecated wrapper. Use ``TomlFile().read(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the TOML file on disk.

    Returns
    -------
    JSONData
        The structured data read from the TOML file.
    """
    return call_deprecated_module_read(
        path,
        __name__,
        _TOML_HANDLER.read,
    )


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Deprecated wrapper. Use ``TomlFile().write(...)`` instead.

    Parameters
    ----------
    path : StrPath
        Path to the TOML file on disk.
    data : JSONData
        Data to write as TOML. Should be a dictionary.

    Returns
    -------
    int
        The number of records written to the TOML file.
    """
    return call_deprecated_module_write(
        path,
        data,
        __name__,
        _TOML_HANDLER.write,
    )
