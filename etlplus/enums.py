"""
ETLPlus Enums
=======================

Shared enumeration types used across ETLPlus modules.
"""
from __future__ import annotations

import enum


# SECTION: ENUMS ============================================================ #


class DataConnectorType(enum.StrEnum):
    """
    Supported data connector types.
    """

    FILE = 'file'
    DATABASE = 'database'
    API = 'api'


class FileFormat(enum.StrEnum):
    """
    Supported file formats for extraction.
    """

    CSV = 'csv'
    JSON = 'json'
    XML = 'xml'


class HttpMethod(enum.StrEnum):
    """
    HTTP verbs that accept JSON payloads.
    """

    POST = 'post'
    PUT = 'put'
    PATCH = 'patch'


# SECTION: FUNCTIONS ======================================================== #


def coerce_data_connector_type(
    connector: DataConnectorType | str,
) -> DataConnectorType:
    """
    Normalize textual data connector values to :class:`DataConnectorType`.

    Parameters
    ----------
    connector : DataConnectorType | str
        Connector type to normalize.

    Returns
    -------
    DataConnectorType
        Normalized connector type.

    Raises
    ------
    ValueError
        If the connector type is invalid.
    """

    if isinstance(connector, DataConnectorType):
        return connector
    try:
        return DataConnectorType(str(connector).lower())
    except ValueError as e:
        raise ValueError(f'Invalid data connector type: {connector}') from e


def coerce_file_format(
    file_format: FileFormat | str,
) -> FileFormat:
    """
    Normalize textual file format values to :class:`FileFormat`.

    Parameters
    ----------
    file_format : FileFormat | str
        File format to normalize.

    Returns
    -------
    FileFormat
        Normalized file format.

    Raises
    ------
    ValueError
        If the file format is unsupported.
    """

    if isinstance(file_format, FileFormat):
        return file_format
    try:
        return FileFormat(str(file_format).lower())
    except ValueError as e:
        raise ValueError(f'Unsupported format: {file_format}') from e
