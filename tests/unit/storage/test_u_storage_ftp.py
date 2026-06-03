"""
:mod:`tests.unit.storage.test_u_storage_ftp` module.

Unit tests for :mod:`etlplus.storage._ftp`.
"""

from __future__ import annotations

import pytest

from etlplus.storage import FtpStorageBackend
from etlplus.storage import StorageLocation

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestFtpStorageBackend:
    """Unit tests for the FTP storage backend."""

    @pytest.mark.parametrize(
        ('method_name', 'args', 'kwargs'),
        [
            ('exists', (), {}),
            ('delete', (), {}),
            ('open', ('rb',), {'newline': None}),
        ],
    )
    def test_ftp_routes_through_placeholder_behavior(
        self,
        method_name: str,
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> None:
        """Test that FTP operations route through shared placeholder behavior."""
        backend = FtpStorageBackend()
        location = StorageLocation.from_value('ftp://example.com/data.json')
        with pytest.raises(NotImplementedError, match='ftplib'):
            getattr(backend, method_name)(location, *args, **kwargs)
