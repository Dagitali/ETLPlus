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
from pathlib import Path
from typing import Any
from typing import cast

from ..types import JSONData
from ..types import JSONDict
from ..types import StrPath
from ._imports import get_optional_module
from ._io import coerce_path
from ._io import read_text
from ._io import require_dict_payload
from ._io import write_text
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

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read and return TOML content from *path*.

        Parameters
        ----------
        path : Path
            Path to the TOML file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            The structured data read from the TOML file.
        """
        encoding = self.encoding_from_read_options(options)
        return self.loads(read_text(path, encoding=encoding), options=options)

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* to TOML at *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the TOML file on disk.
        data : JSONData
            Data to write as TOML. Should be a dictionary.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of records written to the TOML file.
        """
        encoding = self.encoding_from_write_options(options)
        write_text(
            path,
            self.dumps(data, options=options),
            encoding=encoding,
        )
        return 1


# SECTION: INTERNAL CONSTANTS =============================================== #

_TOML_HANDLER = TomlFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read and return TOML content from *path*.

    Parameters
    ----------
    path : StrPath
        Path to the TOML file on disk.

    Returns
    -------
    JSONData
        The structured data read from the TOML file.
    """
    return _TOML_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* to TOML at *path* and return record count.

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
    return _TOML_HANDLER.write(coerce_path(path), data)
