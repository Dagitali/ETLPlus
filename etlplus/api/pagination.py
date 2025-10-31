"""
ETLPlus API Pagination
======================

REST API helpers for pagination.
"""
from __future__ import annotations

import warnings
from typing import Any
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from .client import EndpointClient


# SECTION: FUNCTIONS ======================================================== #


def paginate(
    url: str,
    params: dict[str, Any] | None,
    headers: dict[str, Any] | None,
    timeout: float | int | None,
    pagination: dict[str, Any] | None,
    *,
    sleep_seconds: float = 0.0,
) -> Any:
    """
    DEPRECATED: Use EndpointClient.paginate_url instead.

    This function is retained as a shim for backward compatibility and will
    be removed in a future major release. It delegates to
    EndpointClient.paginate_url.
    """
    warnings.warn(
        (
            'etlplus.api.pagination.paginate is deprecated and will be '
            'removed in a future release; use '
            'etlplus.api.client.EndpointClient.paginate_url instead.'
        ),
        DeprecationWarning,
        stacklevel=2,
    )

    parts = urlsplit(url)
    base_url = urlunsplit((parts.scheme, parts.netloc, '', '', ''))
    client = EndpointClient(base_url=base_url, endpoints={})
    return client.paginate_url(
        url,
        params,
        headers,
        timeout,
        pagination,
        sleep_seconds=sleep_seconds,
    )
