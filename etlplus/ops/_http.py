"""
:mod:`etlplus.ops._http` module.

HTTP request helpers shared by extract/load orchestration.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import TypedDict
from typing import cast

from ..api import HttpMethod
from ..api._utils import ApiRequestEnvDict
from ..api._utils import ApiTargetEnvDict
from ..api._utils import resolve_request
from ..utils import MappingFieldParser
from ..utils._types import JSONData
from ..utils._types import Timeout

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'ResolvedRequest',
    # Functions
    'build_direct_request_env',
    'build_request_call',
    'require_url',
    'send_request',
    'response_json_or_text',
    # Type Aliases
    'HttpRequestEnv',
    # Typed Dicts
    'DirectRequestEnvDict',
]


# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class ResolvedRequest:
    """Normalized HTTP request call details."""

    # -- Instance Attributes -- #

    url: str
    request_callable: Callable[..., Any]
    timeout: Any
    http_method: HttpMethod
    kwargs: dict[str, Any]


# SECTION: TYPED DICTS ====================================================== #


class DirectRequestEnvDict(TypedDict, total=False):
    """Normalized environment for one direct HTTP request."""

    url: str | None
    headers: dict[str, str]
    timeout: Timeout
    session: Any | None
    method: HttpMethod | str | None
    request_kwargs: dict[str, Any]


# SECTION: TYPE ALIASES ===================================================== #


type HttpRequestEnv = ApiRequestEnvDict | ApiTargetEnvDict | DirectRequestEnvDict


# SECTION: FUNCTIONS ======================================================== #


def build_direct_request_env(
    url: str,
    method: HttpMethod | str,
    kwargs: Mapping[str, Any] | None = None,
) -> DirectRequestEnvDict:
    """
    Build one normalized environment for a direct HTTP request.

    Parameters
    ----------
    url : str
        Request URL.
    method : HttpMethod | str
        HTTP method for the request.
    kwargs : Mapping[str, Any] | None, optional
        Request keyword arguments supplied by the caller. ``timeout`` and
        ``session`` are promoted to top-level environment fields; remaining
        values are preserved under ``request_kwargs``.

    Returns
    -------
    DirectRequestEnvDict
        Normalized request environment compatible with
        :func:`build_request_call`.
    """
    request_kwargs = dict(kwargs or {})
    timeout = request_kwargs.pop('timeout', None)
    session = request_kwargs.pop('session', None)
    return {
        'url': url,
        'method': method,
        'timeout': timeout,
        'session': session,
        'request_kwargs': request_kwargs,
    }


def require_url(
    env: HttpRequestEnv,
    *,
    error_message: str,
) -> str:
    """
    Return one required URL string from a normalized request environment.

    Parameters
    ----------
    env : HttpRequestEnv
        A normalized request environment mapping, expected to contain a 'url'
        key with a non-empty string value representing the request URL.
    error_message : str
        A descriptive error message to include in the ValueError if the URL is
        missing or invalid.

    Returns
    -------
    str
        The URL string extracted from the environment.

    Raises
    ------
    ValueError
        If the 'url' key is missing from the environment, or if its value is
        not a non-empty string. The error message will include the provided
        `error_message` for context.
    """
    env_map = cast(Mapping[str, Any], env)
    if not (url := MappingFieldParser.required_str(env_map, 'url')):
        raise ValueError(error_message)
    return url


def build_request_call(
    env: HttpRequestEnv,
    *,
    error_message: str,
    default_method: HttpMethod | str,
    json_data: JSONData | None = None,
) -> ResolvedRequest:
    """
    Normalize one direct HTTP request call from a request environment.

    Parameters
    ----------
    env : HttpRequestEnv
        A normalized request environment mapping, expected to contain keys such
        as 'url', 'method', 'headers', 'request_kwargs', 'session', and
        'timeout'.
    error_message : str
        A descriptive error message to include in the ValueError if the URL is
        missing or invalid.
    default_method : HttpMethod | str
        The default HTTP method to use if none is specified in the environment.
    json_data : JSONData | None, optional
        JSON data to include in the request body, if any.

    Returns
    -------
    ResolvedRequest
        A dataclass instance containing the normalized request call details.
    """
    env_map = cast(Mapping[str, Any], env)
    url = require_url(env, error_message=error_message)
    request_kwargs = dict(
        cast(Mapping[str, Any] | None, env_map.get('request_kwargs')) or {},
    )
    headers = env_map.get('headers')
    if isinstance(headers, Mapping):
        request_kwargs.setdefault('headers', headers)

    method = env_map.get('method') or default_method
    session = env_map.get('session')
    request_timeout = env_map.get('timeout')

    request_callable, timeout, http_method = resolve_request(
        method,
        session=session,
        timeout=request_timeout,
    )
    if json_data is not None:
        request_kwargs['json'] = json_data

    return ResolvedRequest(
        url=url,
        request_callable=request_callable,
        timeout=timeout,
        http_method=http_method,
        kwargs=request_kwargs,
    )


def response_json_or_text(
    response: Any,
) -> Any:
    """
    Return :meth:`response.json` when available, else the raw text body.

    Parameters
    ----------
    response : Any
        An HTTP response object, expected to have a :meth:`json` method and
        :attr:`text` attribute.

    Returns
    -------
    Any
        The JSON-decoded response content if available, otherwise the raw text
        content.
    """
    try:
        return response.json()
    except ValueError:
        return response.text


def send_request(
    request: ResolvedRequest,
) -> Any:
    """
    Execute one normalized HTTP request and raise for bad status codes.

    Parameters
    ----------
    request : ResolvedRequest
        Normalized request details produced by :func:`build_request_call`.

    Returns
    -------
    Any
        The HTTP response object returned by the request callable.
    """
    response = request.request_callable(
        request.url,
        timeout=request.timeout,
        **request.kwargs,
    )
    response.raise_for_status()
    return response
