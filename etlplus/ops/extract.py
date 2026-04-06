"""
:mod:`etlplus.ops.extract` module.

Helpers to extract data from files, databases, and REST APIs.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from typing import cast
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from ..api import EndpointClient
from ..api import HttpMethod
from ..api import RequestOptions
from ..api import compose_api_request_env
from ..api import paginate_with_client
from ..api._utils import ApiRequestEnvDict
from ..connector import DataConnectorType
from ..file import File
from ..file import FileFormat
from ..file._core import FileFormatArg
from ..file.base import ReadOptions
from ..utils._types import JSONData
from ..utils._types import JSONList
from ..utils._types import StrPath
from ..utils._types import Timeout
from ._database import DATABASE_DRIVER_NOTE
from ._database import DATABASE_EXTRACT_NOT_IMPLEMENTED
from ._files import resolve_file
from ._http import DirectRequestEnvDict
from ._http import build_direct_request_env
from ._http import build_request_call
from ._http import require_url
from ._http import send_request
from ._options import coerce_read_options as _coerce_read_options
from ._types import ConnectorTypeArg
from ._types import FileOptionsArg

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
    env: ApiRequestEnvDict | DirectRequestEnvDict,
    *,
    use_client: bool,
) -> JSONData:
    """
    Extract API data from a normalized request environment.

    Parameters
    ----------
    env : ApiRequestEnvDict | DirectRequestEnvDict
        Normalized environment describing API request parameters.
    use_client : bool
        Whether to use the endpoint client/pagination machinery.

    Returns
    -------
    JSONData
        Extracted payload.
    """
    if use_client:
        request_env = cast(ApiRequestEnvDict, env)
    else:
        request_env = None

    if (
        request_env is not None
        and request_env.get('use_endpoints')
        and request_env.get('base_url')
        and request_env.get('endpoints_map')
        and request_env.get('endpoint_key')
    ):
        client = _build_client(
            base_url=str(request_env['base_url']),
            base_path=(
                base_path
                if isinstance(base_path := request_env.get('base_path'), str)
                else None
            ),
            endpoints=dict(request_env.get('endpoints_map') or {}),
            retry=request_env.get('retry'),
            retry_network_errors=bool(
                request_env.get('retry_network_errors', False),
            ),
            session=request_env.get('session'),
        )
        return paginate_with_client(
            client,
            str(request_env['endpoint_key']),
            request_env.get('params'),
            request_env.get('headers'),
            request_env.get('timeout'),
            request_env.get('pagination'),
            request_env.get('sleep_seconds'),
        )

    if request_env is not None:
        url = require_url(
            request_env,
            error_message='API source missing URL',
        )
        parts = urlsplit(url)
        base = urlunsplit((parts.scheme, parts.netloc, '', '', ''))
        client = _build_client(
            base_url=base,
            base_path=None,
            endpoints={},
            retry=request_env.get('retry'),
            retry_network_errors=bool(
                request_env.get('retry_network_errors', False),
            ),
            session=request_env.get('session'),
        )
        request_options = RequestOptions(
            params=cast(Mapping[str, Any] | None, request_env.get('params')),
            headers=cast(Mapping[str, str] | None, request_env.get('headers')),
            timeout=cast(Timeout | None, request_env.get('timeout')),
        )

        return client.paginate_url(
            url,
            request_env.get('pagination'),
            request=request_options,
            sleep_seconds=float(request_env.get('sleep_seconds', 0.0)),
        )

    request = build_request_call(
        env,
        error_message='API source missing URL',
        default_method=HttpMethod.GET,
    )
    return _parse_api_response(send_request(request))


def _parse_api_response(
    response: Any,
) -> JSONData:
    """
    Parse API responses into a consistent JSON payload.

    Parameters
    ----------
    response : Any
        HTTP response object exposing :meth:`headers`, :meth:`json`, and
        :meth:`text`.

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
        match payload:
            case dict():
                return payload
            case list():
                if all(isinstance(item, dict) for item in payload):
                    return payload
                return [{'value': item} for item in payload]
            case _:
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
    env = build_direct_request_env(url, method, kwargs)
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
            'message': DATABASE_EXTRACT_NOT_IMPLEMENTED,
            'connection_string': connection_string,
            'note': DATABASE_DRIVER_NOTE,
        },
    ]


def extract_from_file(
    file_path: StrPath,
    file_format: FileFormatArg = FileFormat.JSON,
    options: FileOptionsArg[ReadOptions] = None,
) -> JSONData:
    """
    Extract (semi-)structured data from a local file path or remote URI.

    Parameters
    ----------
    file_path : StrPath
        Source local file path or remote URI.
    file_format : FileFormatArg, optional
        File format to parse. If ``None``, infer from the filename
        extension. Defaults to `'json'` for backward compatibility when
        explicitly provided.
    options : FileOptionsArg[ReadOptions], optional
        Optional file-read options such as ``encoding`` plus format-specific
        extras like ``delimiter``.

    Returns
    -------
    JSONData
        Parsed data as a mapping or a list of mappings.
    """
    resolved_options = _coerce_read_options(options)
    source = resolve_file(
        file_path,
        file_format,
        file_cls=File,
    )
    return (
        source.file.read()
        if resolved_options is None
        else source.file.read(options=resolved_options)
    )


# -- Orchestration -- #


def extract(
    source_type: ConnectorTypeArg,
    source: StrPath,
    file_format: FileFormatArg = None,
    **kwargs: Any,
) -> JSONData:
    """
    Extract data from a source (file, database, or API).

    Parameters
    ----------
    source_type : ConnectorTypeArg
        Type of data source.
    source : StrPath
        Source location (file path, connection string, or API URL).
    file_format : FileFormatArg, optional
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
            return extract_from_file(source, file_format, kwargs or None)
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
