"""
:mod:`etlplus.storage._ftp` module.

FTP storage backend skeleton.
"""

from __future__ import annotations

from ._enums import StorageScheme
from ._stub import StubStorageBackend

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'FtpStorageBackend',
]


# SECTION: CLASSES ========================================================== #


class FtpStorageBackend(StubStorageBackend):
    """
    Storage backend skeleton for ``ftp://host/path`` locations.

    This backend currently validates FTP-shaped locations and reserves the
    public runtime surface for a future SDK-backed implementation.
    """

    authority_label = 'host'
    package_name = 'ftplib'
    path_label = 'remote path'
    scheme = StorageScheme.FTP
    service_name = 'FTP'
