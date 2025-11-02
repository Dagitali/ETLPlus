"""
etlplus.api.transport module
============================

Lightweight helpers for configuring HTTP transport, kept separate to avoid
duplication across modules.

Functions
---------
- build_http_adapter(cfg): Build a requests HTTPAdapter from a simple mapping.
"""
from __future__ import annotations

from typing import Any
from typing import Mapping

from requests.adapters import HTTPAdapter  # type: ignore


# SECTION: PUBLIC API ======================================================= #


__all__ = ['build_http_adapter']


# SECTION: FUNCTIONS ======================================================== #


def build_http_adapter(cfg: Mapping[str, Any]) -> HTTPAdapter:
    """
    Build a requests HTTPAdapter from a configuration mapping.

    Supported keys in cfg:
    - pool_connections (int)
    - pool_maxsize (int)
    - pool_block (bool)
    - max_retries (int or dict matching urllib3 Retry args)

    When ``max_retries`` is a dict, this will attempt to construct an
    ``urllib3.util.retry.Retry`` instance with the provided keys. Unknown
    keys are ignored. If urllib3 is unavailable, falls back to no retries
    (0) or an integer value when provided.

    Parameters
    ----------
    cfg : Mapping[str, Any]
        Adapter configuration mapping.

    Returns
    -------
    HTTPAdapter
        Configured HTTPAdapter instance.
    """

    pool_connections = cfg.get('pool_connections')
    try:
        pool_connections_i = (
            int(pool_connections) if pool_connections is not None else 10
        )
    except (TypeError, ValueError):
        pool_connections_i = 10

    pool_maxsize = cfg.get('pool_maxsize')
    try:
        pool_maxsize_i = int(pool_maxsize) if pool_maxsize is not None else 10
    except (TypeError, ValueError):
        pool_maxsize_i = 10

    pool_block = bool(cfg.get('pool_block', False))

    retries_cfg = cfg.get('max_retries')
    max_retries: Any
    if isinstance(retries_cfg, int):
        max_retries = retries_cfg
    elif isinstance(retries_cfg, dict):
        # Try to construct urllib3 Retry from dict
        try:
            from urllib3.util.retry import Retry  # type: ignore

            allowed_keys = {
                'total',
                'connect',
                'read',
                'redirect',
                'status',
                'backoff_factor',
                'status_forcelist',
                'allowed_methods',
                'raise_on_status',
                'respect_retry_after_header',
            }
            kwargs: dict[str, Any] = {}
            for k, v in retries_cfg.items():
                if (
                    k in {'status_forcelist', 'allowed_methods'}
                    and isinstance(v, (list, tuple, set))
                ):
                    # Convert to tuple/set as appropriate
                    kwargs[k] = (
                        tuple(v) if k == 'status_forcelist' else frozenset(v)
                    )
                elif k in allowed_keys:
                    kwargs[k] = v
            max_retries = Retry(**kwargs) if kwargs else 0
        except (ImportError, TypeError, ValueError, AttributeError):
            # Fallback if urllib3 not available or invalid config
            total = (
                retries_cfg.get('total')
                if isinstance(retries_cfg.get('total'), int)
                else 0
            )
            max_retries = int(total) if isinstance(total, int) else 0
    else:
        max_retries = 0

    return HTTPAdapter(
        pool_connections=pool_connections_i,
        pool_maxsize=pool_maxsize_i,
        max_retries=max_retries,
        pool_block=pool_block,
    )
