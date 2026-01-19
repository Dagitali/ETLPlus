"""
:mod:`etlplus.ops.http_utils` module.

Shared HTTP helpers for ETL ops modules that communicate with REST APIs.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import requests  # type: ignore[import]

from ..enums import HttpMethod
from ..types import Timeout

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Constants
    'DEFAULT_TIMEOUT',
    # Functions
    'resolve_request',
]


# SECTION: CONSTANTS ======================================================== #


DEFAULT_TIMEOUT: float = 10.0


# SECTION: FUNCTIONS ======================================================== #


def resolve_request(
    method: HttpMethod | str,
    *,
    session: Any | None = None,
    timeout: Timeout = None,
) -> tuple[Callable[..., requests.Response], float, HttpMethod]:
    """
    Resolve a request callable and effective timeout for an HTTP method.

    Parameters
    ----------
    method : HttpMethod | str
        HTTP method to execute.
    session : Any | None, optional
        Requests-compatible session object. Defaults to module-level
        ``requests``.
    timeout : Timeout, optional
        Timeout in seconds for the request. Uses ``DEFAULT_TIMEOUT`` when
        omitted.

    Returns
    -------
    tuple[Callable[..., requests.Response], float, HttpMethod]
        Tuple of (callable, timeout_seconds, resolved_method).

    Raises
    ------
    TypeError
        If the session object does not expose the requested HTTP method.
    """
    http_method = HttpMethod.coerce(method)
    request_timeout = DEFAULT_TIMEOUT if timeout is None else timeout
    requester = session or requests
    request_callable = getattr(requester, http_method.value, None)
    if not callable(request_callable):
        raise TypeError(
            'Session object must supply a callable '
            f'"{http_method.value}" method',
        )
    return request_callable, request_timeout, http_method
