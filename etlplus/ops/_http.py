"""
:mod:`etlplus.ops._http` module.

HTTP request helpers shared by extract/load orchestration.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ..api import HttpMethod
from ..api._utils import resolve_request
from ..utils._types import JSONData

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Data Classes
    'ResolvedRequest',
    # Functions
    'build_request_call',
    'require_url',
    'response_json_or_text',
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


# SECTION: FUNCTIONS ======================================================== #


def require_url(
    env: Mapping[str, Any],
    *,
    error_message: str,
) -> str:
    """
    Return one required URL string from a normalized request environment.

    Parameters
    ----------
    env : Mapping[str, Any]
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
    url = env.get('url')
    if not isinstance(url, str) or not url:
        raise ValueError(error_message)
    return url


def build_request_call(
    env: Mapping[str, Any],
    *,
    error_message: str,
    default_method: HttpMethod | str,
    json_data: JSONData | None = None,
) -> ResolvedRequest:
    """
    Normalize one direct HTTP request call from a request environment.

    Parameters
    ----------
    env : Mapping[str, Any]
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
    kwargs = dict(env.get('request_kwargs') or {})
    headers = env.get('headers')
    if isinstance(headers, Mapping):
        kwargs.setdefault('headers', headers)

    request_callable, timeout, http_method = resolve_request(
        env.get('method') or default_method,
        session=env.get('session'),
        timeout=env.get('timeout'),
    )
    if json_data is not None:
        kwargs['json'] = json_data

    return ResolvedRequest(
        url=require_url(env, error_message=error_message),
        request_callable=request_callable,
        timeout=timeout,
        http_method=http_method,
        kwargs=kwargs,
    )


def response_json_or_text(
    response: Any,
) -> Any:
    """
    Return ``response.json()`` when available, else the raw text body.

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
