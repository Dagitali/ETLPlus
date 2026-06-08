"""
:mod:`tests.integration.storage.test_i_storage_remote` module.

Env-gated integration tests for remote storage-backed file IO.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from etlplus.file import File
from etlplus.file import FileFormat
from etlplus.storage import StorageLocation
from etlplus.storage import get_backend
from tests.integration.pytest_integration_support import REMOTE_STORAGE_ENV_CASES
from tests.integration.pytest_integration_support import child_uri
from tests.integration.pytest_integration_support import require_env

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    'env_name',
    REMOTE_STORAGE_ENV_CASES,
)
def test_json_roundtrip_via_file_api(
    env_name: str,
) -> None:
    """Round-trip JSON through a real remote object when credentials are set."""
    base_uri = require_env(env_name)
    target_uri = child_uri(base_uri, f'etlplus-{uuid4().hex}.json')
    payload = [{'name': 'Ada'}]
    location = StorageLocation.from_value(target_uri)
    backend = get_backend(location)

    try:
        written = File(target_uri, FileFormat.JSON).write(payload)
        result = File(target_uri, FileFormat.JSON).read()

        assert written == 1
        assert result == payload
        assert backend.exists(location)
    finally:
        backend.delete(location)
