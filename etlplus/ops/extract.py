"""
:mod:`etlplus.ops.extract` module.

Helpers to extract data from files, databases, and REST APIs.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import cast
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from ..api import EndpointClient
from ..api import HttpMethod
from ..api import PaginationConfigMap
from ..api import RequestOptions
from ..api import compose_api_request_env
from ..api import paginate_with_client
from ..api.utils import resolve_request
from ..connector import DataConnectorType
from ..file import File
from ..file import FileFormat
from ..types import JSONData
from ..types import JSONDict
from ..types import JSONList
from ..types import StrPath
from ..types import Timeout

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Functions
    'extract',
    'extract_from_api',
    'extract_from_database',
    'extract_from_file',
]


# SECTION: INTERNAL FUNCTIONS =============================================== #


def _build_client(
    *,
    base_url: str,
    base_path: str | None,
    endpoints: dict[str, str],
    retry: Any,
    retry_network_errors: bool,
    session: Any,
) -> EndpointClient:
    """
    Construct an API client with shared defaults.

    Parameters
    ----------
    base_url : str
        API base URL.
    base_path : str | None
        Base path to prepend for endpoints.
    endpoints : dict[str, str]
        Endpoint name to path mappings.
    retry : Any
        Retry policy configuration.
    retry_network_errors : bool
        Whether to retry on network errors.
    session : Any
        Optional requests session.

    Returns
    -------
    EndpointClient
        Configured endpoint client instance.
    """
    ClientClass = EndpointClient  # noqa: N806
    return ClientClass(
        base_url=base_url,
        base_path=base_path,
        endpoints=endpoints,
        retry=retry,
        retry_network_errors=retry_network_errors,
        session=session,
    )


def _extract_from_api_env(
    env: Mapping[str, Any],
    *,
    use_client: bool,
) -> JSONData:
    """
    Extract API data from a normalized request environment.

    Parameters
    ----------
    env : Mapping[str, Any]
        Normalized environment describing API request parameters.
    use_client : bool
        Whether to use the endpoint client/pagination machinery.

    Returns
    -------
    JSONData
        Extracted payload.

    Raises
    ------
    ValueError
        If required parameters are missing.
    """
    if (
        use_client
        and env.get('use_endpoints')
        and env.get('base_url')
        and env.get('endpoints_map')
        and env.get('endpoint_key')
    ):
        client = _build_client(
            base_url=cast(str, env.get('base_url')),
            base_path=cast(str | None, env.get('base_path')),
            endpoints=cast(dict[str, str], env.get('endpoints_map', {})),
            retry=env.get('retry'),
            retry_network_errors=bool(env.get('retry_network_errors', False)),
            session=env.get('session'),
        )
        return paginate_with_client(
            client,
            cast(str, env.get('endpoint_key')),
            env.get('params'),
            env.get('headers'),
            env.get('timeout'),
            env.get('pagination'),
            cast(float | None, env.get('sleep_seconds')),
        )

    url = env.get('url')
    if not url:
        raise ValueError('API source missing URL')

    if use_client:
        parts = urlsplit(cast(str, url))
        base = urlunsplit((parts.scheme, parts.netloc, '', '', ''))
        client = _build_client(
            base_url=base,
            base_path=None,
            endpoints={},
            retry=env.get('retry'),
            retry_network_errors=bool(env.get('retry_network_errors', False)),
            session=env.get('session'),
        )
        request_options = RequestOptions(
            params=cast(Mapping[str, Any] | None, env.get('params')),
            headers=cast(Mapping[str, str] | None, env.get('headers')),
            timeout=cast(Timeout | None, env.get('timeout')),
        )

        return client.paginate_url(
            cast(str, url),
            cast(PaginationConfigMap | None, env.get('pagination')),
            request=request_options,
            sleep_seconds=cast(float, env.get('sleep_seconds', 0.0)),
        )

    method = env.get('method', HttpMethod.GET)
    timeout = env.get('timeout', None)
    session = env.get('session', None)
    request_kwargs = dict(env.get('request_kwargs') or {})
    request_callable, timeout, _ = resolve_request(
        method,
        session=session,
        timeout=timeout,
    )
    response = request_callable(
        cast(str, url),
        timeout=timeout,
        **request_kwargs,
    )
    response.raise_for_status()
    return _parse_api_response(response)


def _parse_api_response(
    response: Any,
) -> JSONData:
    """
    Parse API responses into a consistent JSON payload.

    Parameters
    ----------
    response : Any
        HTTP response object exposing ``headers``, ``json()``, and ``text``.

    Returns
    -------
    JSONData
        Parsed JSON payload, or a fallback object with raw text.
    """
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
    """
    env = {
        'url': url,
        'method': method,
        'timeout': kwargs.pop('timeout', None),
        'session': kwargs.pop('session', None),
        'request_kwargs': kwargs,
    }
    return _extract_from_api_env(env, use_client=False)


def extract_from_api_source(
    cfg: Any,
    source_obj: Any,
    overrides: dict[str, Any],
) -> JSONData:
    """
    Extract data from a REST API source connector.

    Parameters
    ----------
    cfg : Any
        Pipeline configuration.
    source_obj : Any
        Connector configuration.
    overrides : dict[str, Any]
        Extract-time overrides.

    Returns
    -------
    JSONData
        Extracted payload.
    """
    env = compose_api_request_env(cfg, source_obj, overrides)
    return _extract_from_api_env(env, use_client=True)


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
