"""
ETLPlus API
===========

REST API helpers package for ETLPlus (pagination, request utils).
"""
from __future__ import annotations

from .auth import EndpointCredentialsBearer
from .client import EndpointClient
from .pagination import paginate
from .request import compute_sleep_seconds


# SECTION: PUBLIC API ======================================================= #


__all__ = [
    'EndpointCredentialsBearer',
    'EndpointClient',
    'compute_sleep_seconds',
    'paginate',
]
