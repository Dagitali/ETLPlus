"""
:mod:`etlplus.storage` package.

Storage location and backend helpers.
"""

from __future__ import annotations

from .base import StorageBackend
from .enums import StorageScheme
from .local import LocalStorageBackend
from .location import StorageLocation
from .registry import coerce_location
from .registry import get_backend

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'LocalStorageBackend',
    'StorageBackend',
    'StorageLocation',
    # Enums
    'StorageScheme',
    # Functions
    'coerce_location',
    'get_backend',
]
