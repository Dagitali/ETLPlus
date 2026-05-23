"""
:mod:`tests.integration.pytest_integration_support` module.

Shared support types and helpers for integration tests.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from typing import Protocol
from typing import cast

import pytest

from etlplus import Config

# SECTION: PROTOCOLS ======================================================== #


class FakeEndpointClientProtocol(Protocol):
    """
    Protocol for fake endpoint clients used in integration tests.

    Attributes
    ----------
    seen : dict[str, Any]
        Dictionary capturing values observed during pagination.
    """

    seen: dict[str, Any]


# SECTION: TYPE ALIASES ===================================================== #


type FakeEndpointClients = tuple[
    type[FakeEndpointClientProtocol],
    list[FakeEndpointClientProtocol],
]
type PipelineCfgFactory = Callable[..., Config]
type RunPatched = Callable[..., dict[str, Any]]
type StdinText = Callable[[str], None]


# SECTION: CONSTANTS ======================================================== #


REMOTE_STORAGE_ENV_CASES = (
    pytest.param('ETLPLUS_TEST_S3_URI', id='s3'),
    pytest.param('ETLPLUS_TEST_AZURE_BLOB_URI', id='azure-blob'),
)


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class RemoteStorageHarness:
    """In-memory remote object store for integration tests."""

    objects: dict[str, bytes]
    writes: list[tuple[str, bytes]]

    def set_text(self, uri: str, payload: str) -> None:
        """Store UTF-8 text content at a remote URI."""
        self.objects[uri] = payload.encode('utf-8')

    def set_json(self, uri: str, payload: Any) -> None:
        """Store JSON content at a remote URI."""
        self.set_text(uri, json.dumps(payload))

    def read_text(self, uri: str) -> str:
        """Return UTF-8 decoded remote object content."""
        return self.objects[uri].decode('utf-8')

    def read_json(self, uri: str) -> Any:
        """Parse remote object content as JSON."""
        return json.loads(self.read_text(uri))


# SECTION: FUNCTIONS ======================================================== #


def child_uri(base_uri: str, filename: str) -> str:
    """
    Append one test filename to a remote base URI.

    Parameters
    ----------
    base_uri : str
        Remote base URI supplied by the integration-test environment.
    filename : str
        Test object filename to append.

    Returns
    -------
    str
        Remote child URI.
    """
    return f'{base_uri.rstrip("/")}/{filename}'


def require_env(name: str) -> str:
    """
    Return one required env var or skip the integration test.

    Parameters
    ----------
    name : str
        Environment variable name to read.

    Returns
    -------
    str
        Configured environment variable value.

    Example safe placeholder values:
    - ``ETLPLUS_TEST_S3_URI=s3://my-etlplus-integration-bucket/cli``
    - ``ETLPLUS_TEST_AZURE_BLOB_URI=azure-blob://etlplus-integration/cli``

    Real values should be supplied from developer shell config, ``.envrc``,
    VS Code test environment settings, or CI secret stores rather than being
    committed to the repository.
    """
    value = os.getenv(name)
    if not value:
        pytest.skip(f'{name} is not configured for cloud integration tests')
    return cast(str, value)
