"""
:mod:`etlplus.file.yaml` module.

Helpers for reading/writing YAML Ain't Markup Language (YAML) files.

Notes
-----
- A YAML file is a human-readable data serialization format.
- Common cases:
    - Configuration files.
    - Data exchange between languages with different data structures.
    - Complex data storage.
- Rule of thumb:
    - If the file follows the YAML specification, use this module for
        reading and writing.
"""

from __future__ import annotations

from pathlib import Path

from ..types import JSONData
from ..types import StrPath
from ..utils import count_records
from ._imports import get_yaml
from ._io import coerce_path
from ._io import coerce_record_payload
from ._io import ensure_parent_dir
from .base import ReadOptions
from .base import SemiStructuredTextFileHandlerABC
from .base import WriteOptions
from .enums import FileFormat

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'YamlFile',
    # Functions
    'read',
    'write',
]


# SECTION: CLASSES ========================================================== #


class YamlFile(SemiStructuredTextFileHandlerABC):
    """
    Handler implementation for YAML files.
    """

    # -- Class Attributes -- #

    format = FileFormat.YAML

    # -- Instance Methods -- #

    def dumps(
        self,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> str:
        """
        Serialize *data* to YAML text.

        Parameters
        ----------
        data : JSONData
            Payload to serialize.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        str
            Serialized YAML text.
        """
        _ = options
        return str(
            get_yaml().safe_dump(
                data,
                sort_keys=False,
                allow_unicode=True,
                default_flow_style=False,
            ),
        )

    def loads(
        self,
        text: str,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Parse YAML *text* into structured records.

        Parameters
        ----------
        text : str
            YAML payload as text.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            Parsed payload.
        """
        _ = options
        loaded = get_yaml().safe_load(text)
        return coerce_record_payload(loaded, format_name='YAML')

    def read(
        self,
        path: Path,
        *,
        options: ReadOptions | None = None,
    ) -> JSONData:
        """
        Read YAML content from *path*.

        Validates that the YAML root is a dict or a list of dicts.

        Parameters
        ----------
        path : Path
            Path to the YAML file on disk.
        options : ReadOptions | None, optional
            Optional read parameters.

        Returns
        -------
        JSONData
            The structured data read from the YAML file.
        """
        encoding = options.encoding if options is not None else 'utf-8'
        with path.open('r', encoding=encoding) as handle:
            loaded = get_yaml().safe_load(handle)
        return coerce_record_payload(loaded, format_name='YAML')

    def write(
        self,
        path: Path,
        data: JSONData,
        *,
        options: WriteOptions | None = None,
    ) -> int:
        """
        Write *data* as YAML to *path* and return record count.

        Parameters
        ----------
        path : Path
            Path to the YAML file on disk.
        data : JSONData
            Data to write as YAML.
        options : WriteOptions | None, optional
            Optional write parameters.

        Returns
        -------
        int
            The number of records written.
        """
        encoding = options.encoding if options is not None else 'utf-8'
        ensure_parent_dir(path)
        with path.open('w', encoding=encoding) as handle:
            get_yaml().safe_dump(
                data,
                handle,
                sort_keys=False,
                allow_unicode=True,
                default_flow_style=False,
            )
        return count_records(data)


# SECTION: INTERNAL CONSTANTS ============================================== #


_YAML_HANDLER = YamlFile()


# SECTION: FUNCTIONS ======================================================== #


def read(
    path: StrPath,
) -> JSONData:
    """
    Read YAML content from *path*.

    Validates that the YAML root is a dict or a list of dicts.

    Parameters
    ----------
    path : StrPath
        Path to the YAML file on disk.

    Returns
    -------
    JSONData
        The structured data read from the YAML file.
    """
    return _YAML_HANDLER.read(coerce_path(path))


def write(
    path: StrPath,
    data: JSONData,
) -> int:
    """
    Write *data* as YAML to *path* and return record count.

    Parameters
    ----------
    path : StrPath
        Path to the YAML file on disk.
    data : JSONData
        Data to write as YAML.

    Returns
    -------
    int
        The number of records written.
    """
    return _YAML_HANDLER.write(coerce_path(path), data)
