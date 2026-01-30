"""
:mod:`etlplus.ops.extract` module.

Helpers to extract data from files, databases, and REST APIs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from typing import cast

from ..api import HttpMethod
from ..api.utils import resolve_request
from ..connector import DataConnectorType
from ..file import File
from ..file import FileFormat
from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'extract',
    'extract_from_api',
    'extract_from_database',
    'extract_from_file',
]


# SECTION: FUNCTIONS ======================================================== #


def extract_from_api(
    url: str,
    method: HttpMethod | str = HttpMethod.GET,
    **kwargs: Any,
) -> JSONData:
    """
    Extract data from a REST API.

    Parameters
    ----------
    url : str
        API endpoint URL.
    method : HttpMethod | str, optional
        HTTP method to use. Defaults to ``GET``.
    **kwargs : Any
        Extra arguments forwarded to the underlying ``requests`` call
        (for example, ``timeout``). To use a pre-configured
        :class:`requests.Session`, provide it via ``session``.
        When omitted, ``timeout`` defaults to 10 seconds.

    Returns
    -------
    JSONData
        Parsed JSON payload, or a fallback object with raw text.

    Raises
    ------
    TypeError
        If a provided ``session`` does not expose the required HTTP
        method (for example, ``get``).
    """
    timeout = kwargs.pop('timeout', None)
    session = kwargs.pop('session', None)
    request_callable, timeout, _ = resolve_request(
        method,
        session=session,
        timeout=timeout,
    )
    response = request_callable(url, timeout=timeout, **kwargs)
    response.raise_for_status()

    content_type = response.headers.get('content-type', '').lower()
    if 'application/json' in content_type:
        try:
            payload: Any = response.json()
        except ValueError:
            # Malformed JSON despite content-type; fall back to text
            return {
                'content': response.text,
                'content_type': content_type,
            }
        if isinstance(payload, dict):
            return cast(JSONDict, payload)
        if isinstance(payload, list):
            if all(isinstance(x, dict) for x in payload):
                return cast(JSONList, payload)
            # Coerce non-dict array items into objects for consistency
            return [{'value': x} for x in payload]
        # Fallback: wrap scalar JSON
        return {'value': payload}

    return {'content': response.text, 'content_type': content_type}


def extract_from_database(
    connection_string: str,
) -> JSONList:
    """
    Extract data from a database.

    Notes
    -----
    Placeholder implementation. To enable database extraction, install and
    configure database-specific drivers and query logic.

    Parameters
    ----------
    connection_string : str
        Database connection string.

    Returns
    -------
    JSONList
        Informational message payload.
    """
    return [
        {
            'message': 'Database extraction not yet implemented',
            'connection_string': connection_string,
            'note': (
                'Install database-specific drivers to enable this feature'
            ),
        },
    ]


def extract_from_file(
    file_path: StrPath,
    file_format: FileFormat | str | None = FileFormat.JSON,
) -> JSONData:
    """
    Extract (semi-)structured data from a local file.

    Parameters
    ----------
    file_path : StrPath
        Source file path.
    file_format : FileFormat | str | None, optional
        File format to parse. If ``None``, infer from the filename
        extension. Defaults to `'json'` for backward compatibility when
        explicitly provided.

    Returns
    -------
    JSONData
        Parsed data as a mapping or a list of mappings.
    """
    path = Path(file_path)

    # If no explicit format is provided, let File infer from extension.
    if file_format is None:
        return File(path, None).read()
    fmt = FileFormat.coerce(file_format)

    # Let file module perform existence and format validation.
    return File(path, fmt).read()


# -- Orchestration -- #


def extract(
    source_type: DataConnectorType | str,
    source: StrPath,
    file_format: FileFormat | str | None = None,
    **kwargs: Any,
) -> JSONData:
    """
    Extract data from a source (file, database, or API).

    Parameters
    ----------
    source_type : DataConnectorType | str
        Type of data source.
    source : StrPath
        Source location (file path, connection string, or API URL).
    file_format : FileFormat | str | None, optional
        File format, inferred from filename extension if omitted.
    **kwargs : Any
        Additional arguments forwarded to source-specific extractors.

    Returns
    -------
    JSONData
        Extracted data.

    Raises
    ------
    ValueError
        If `source_type` is not one of the supported values.
    """
    match DataConnectorType.coerce(source_type):
        case DataConnectorType.FILE:
            # Prefer explicit format if provided, else infer from filename.
            return extract_from_file(source, file_format)
        case DataConnectorType.DATABASE:
            return extract_from_database(str(source))
        case DataConnectorType.API:
            # API extraction always uses an HTTP method; default is GET.
            # ``file_format`` is ignored for APIs.
            return extract_from_api(str(source), **kwargs)
        case _:
            # :meth:`coerce` already raises for invalid connector types, but
            # keep explicit guard for defensive programming.
            raise ValueError(f'Invalid source type: {source_type}')
