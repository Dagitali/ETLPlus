"""
:mod:`tests.integration.storage.test_i_storage_remote` module.

Env-gated integration tests for remote storage-backed file IO.
"""

from __future__ import annotations

import os
from typing import Any
from typing import cast
from uuid import uuid4

import pytest

from etlplus.file import File
from etlplus.file import FileFormat
from etlplus.storage import StorageLocation
from etlplus.storage import get_backend

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def _child_uri(
    base_uri: str,
    filename: str,
) -> str:
    """Append one test filename to a remote base URI."""
    return f'{base_uri.rstrip("/")}/{filename}'


def _require_env(name: str) -> str:
    """Return one required env var or skip the integration test."""
    value = os.getenv(name)
    if not value:
        pytest.skip(f'{name} is not configured for cloud integration tests')
    return cast(str, value)


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
