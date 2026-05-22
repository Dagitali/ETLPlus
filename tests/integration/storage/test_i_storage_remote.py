"""
:mod:`tests.integration.storage.test_i_storage_remote` module.

Env-gated integration tests for remote storage-backed file IO.
"""

from __future__ import annotations

from typing import Any
from typing import cast
from uuid import uuid4

from etlplus.file import File
from etlplus.file import FileFormat
from etlplus.storage import StorageLocation
from etlplus.storage import get_backend
from tests.integration.conftest import _child_uri
from tests.integration.conftest import _require_env

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


def test_s3_json_roundtrip_via_file_api_integration() -> None:
    """Round-trip JSON through a real S3 object when credentials are set."""
    base_uri = _require_env('ETLPLUS_TEST_S3_URI')
    target_uri = _child_uri(base_uri, f'etlplus-{uuid4().hex}.json')
    remote_uri = cast(Any, target_uri)
    payload = [{'name': 'Ada'}]
    location = StorageLocation.from_value(target_uri)
    backend = get_backend(location)

    try:
        written = File(remote_uri, FileFormat.JSON).write(payload)
        result = File(remote_uri, FileFormat.JSON).read()

        assert written == 1
        assert result == payload
        assert backend.exists(location)
    finally:
        backend.delete(location)


def test_azure_blob_json_roundtrip_via_file_api_integration() -> None:
    """Round-trip JSON through a real Azure Blob when credentials are set."""
    base_uri = _require_env('ETLPLUS_TEST_AZURE_BLOB_URI')
    target_uri = _child_uri(base_uri, f'etlplus-{uuid4().hex}.json')
    remote_uri = cast(Any, target_uri)
    payload = [{'name': 'Ada'}]
    location = StorageLocation.from_value(target_uri)
    backend = get_backend(location)

    try:
        written = File(remote_uri, FileFormat.JSON).write(payload)
        result = File(remote_uri, FileFormat.JSON).read()

        assert written == 1
        assert result == payload
        assert backend.exists(location)
    finally:
        backend.delete(location)
