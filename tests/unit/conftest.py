"""
:mod:`tests.unit.conftest` module.

Define shared fixtures and helpers for pytest-based unit tests of
:mod:`etlplus`.

Notes
-----
- Fixtures are designed for reuse and DRY test setup.
"""

from __future__ import annotations

import csv
import itertools
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from etlplus import Config
from etlplus.api import ApiConfig
from etlplus.api import ApiProfileConfig
from etlplus.api import EndpointConfig
from etlplus.api import PaginationConfig
from etlplus.api import PaginationConfigDict
from etlplus.api import RateLimitConfig
from etlplus.api import RateLimitConfigDict
from etlplus.types import JSONData
from tests.unit.pytest_unit_api import api_profile_defaults_factory
from tests.unit.pytest_unit_api import client_factory
from tests.unit.pytest_unit_api import cursor_cfg
from tests.unit.pytest_unit_api import extract_stub_factory
from tests.unit.pytest_unit_api import mock_session
from tests.unit.pytest_unit_api import page_cfg
from tests.unit.pytest_unit_api import request_once_stub
from tests.unit.pytest_unit_api import retry_cfg
from tests.unit.pytest_unit_api import token_sequence

# Re-export shared unit API fixtures for pytest discovery in this scope.
__all__ = [
    'api_profile_defaults_factory',
    'client_factory',
    'cursor_cfg',
    'extract_stub_factory',
    'mock_session',
    'page_cfg',
    'request_once_stub',
    'retry_cfg',
    'token_sequence',
]

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: FIXTURES (CONFIG) ================================================ #


@pytest.fixture
def api_config_factory() -> Callable[[dict[str, Any]], ApiConfig]:
    """
    Create a factory for building ApiConfig from a dictionary.

    Returns
    -------
    Callable[[dict[str, Any]], ApiConfig]
        Function that builds ApiConfig instances from dicts.
    """

    def _make(obj: dict[str, Any]) -> ApiConfig:
        return ApiConfig.from_obj(obj)

    return _make


@pytest.fixture(name='api_obj_factory')
def api_obj_factory_fixture(
    base_url: str,
) -> Callable[..., dict[str, Any]]:
    """
    Create a factory for building API configuration dicts for
    :meth:`ApiConfig.from_obj`.

    Parameters
    ----------
    base_url : str
        Common base URL used across config tests.
    Returns
    -------
    Callable[..., dict[str, Any]]
        Function that builds API configuration dicts for ApiConfig.

    Examples
    --------
    >>> obj = api_obj_factory(base_path='/v1', headers={'X': '1'})
    ... cfg = ApiConfig.from_obj(obj)
    """
    default_endpoints = {
        'users': {'path': '/users'},
        'list': {'path': '/items'},
        'ping': {'path': '/ping'},
    }

    def _make(
        *,
        use_profiles: bool | None = False,
        base_path: str | None = None,
        headers: dict[str, str] | None = None,
        endpoints: dict[str, dict[str, Any]] | None = None,
        defaults: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        eps = endpoints or default_endpoints
        if use_profiles:
            prof: dict[str, Any] = {
                'default': {'base_url': base_url},
            }
            if base_path is not None:
                prof['default']['base_path'] = base_path
            if defaults is not None:
                prof['default']['defaults'] = defaults
            return {
                'profiles': prof,
                'endpoints': eps,
                'headers': headers or {},
            }
        return {
            'base_url': base_url,
            **({'base_path': base_path} if base_path else {}),
            'endpoints': eps,
            **({'headers': headers} if headers else {}),
        }

    return _make


@pytest.fixture
def endpoint_config_factory() -> Callable[[str], EndpointConfig]:
    """
    Create a factory to build :class:`EndpointConfig` from a string path.

    Returns
    -------
    Callable[[str], EndpointConfig]
        Function that builds :class:`EndpointConfig` instances.
    """

    def _make(obj: str) -> EndpointConfig:
        return EndpointConfig.from_obj(obj)

    return _make


@pytest.fixture
def pagination_config_factory() -> Callable[..., PaginationConfig]:
    """
    Create a factory to build :class:`PaginationConfig` via constructor (typed
    kwargs).

    Returns
    -------
    Callable[..., PaginationConfig]
        Function that builds :class:`PaginationConfig` instances.
    """

    def _make(**kwargs: Any) -> PaginationConfig:  # noqa: ANN401
        return PaginationConfig(**kwargs)

    return _make


@pytest.fixture
def pagination_from_obj_factory() -> Callable[
    [Any],
    PaginationConfig,
]:
    """
    Create a factory to build :class:`PaginationConfig` via `from_obj` mapping.

    Returns
    -------
    Callable[[Any], PaginationConfig]
        Function that builds :class:`PaginationConfig` instances from mapping.
    """

    def _make(obj: PaginationConfigDict) -> PaginationConfig:  # noqa: ANN401
        return PaginationConfig.from_obj(obj)

    return _make


@pytest.fixture
def pipeline_yaml_factory() -> Callable[[str, Path], Path]:
    """
    Create a factory to write YAML content to a temporary file and return its
    path.

    Returns
    -------
    Callable[[str, Path], Path]
        Function that writes YAML to a temporary file and returns the path.
    """

    def _make(yaml_text: str, tmp_dir: Path) -> Path:
        p = tmp_dir / 'cfg.yml'
        p.write_text(yaml_text.strip(), encoding='utf-8')
        return p

    return _make


@pytest.fixture
def pipeline_from_yaml_factory() -> Callable[..., Config]:
    """
    Create a factory to build :class:`Config` from a YAML file path.

    Returns
    -------
    Callable[..., Config]
        Function that builds :class:`Config` from a YAML file.
    """

    def _make(
        path: Path,
        *,
        substitute: bool = True,
        env: dict[str, str] | None = None,
    ) -> Config:
        return Config.from_yaml(
            path,
            substitute=substitute,
            env=env or {},
        )

    return _make


@pytest.fixture
def profile_config_factory() -> Callable[[dict[str, Any]], ApiProfileConfig]:
    """
    Create a factory to build :class:`ApiProfileConfig` from a dictionary.

    Returns
    -------
    Callable[[dict[str, Any]], ApiProfileConfig]
        Function that builds :class:`ApiProfileConfig` instances.
    """

    def _make(obj: dict[str, Any]) -> ApiProfileConfig:
        return ApiProfileConfig.from_obj(obj)

    return _make


@pytest.fixture
def rate_limit_config_factory() -> Callable[..., RateLimitConfig]:
    """
    Create a factory to build :class:`RateLimitConfig` via constructor (typed
    kwargs).

    Returns
    -------
    Callable[..., RateLimitConfig]
        Function that builds :class:`RateLimitConfig` instances.
    """

    def _make(**kwargs: Any) -> RateLimitConfig:  # noqa: ANN401
        return RateLimitConfig(**kwargs)

    return _make


@pytest.fixture
def rate_limit_from_obj_factory() -> Callable[
    [RateLimitConfigDict],
    RateLimitConfig,
]:
    """
    Create a factory to build :class:`RateLimitConfig` via `from_obj` mapping.

    Returns
    -------
    Callable[[RateLimitConfigDict], RateLimitConfig]
        Function that builds :class:`RateLimitConfig` from mapping.
    """

    def _make(obj: RateLimitConfigDict) -> RateLimitConfig:
        return RateLimitConfig.from_obj(obj)

    return _make


# SECTION: FIXTURES (FILES) ================================================= #


@pytest.fixture
def csv_writer() -> Callable[[str], None]:
    """
    Create a factory for writing a small CSV file and return its path.

    Returns
    -------
    Callable[[str], None]
        Function that writes a sample CSV file to the given path.
    """

    def _write(path: str) -> None:
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['name', 'age'])
            writer.writeheader()
            writer.writerows(
                [
                    {'name': 'John', 'age': '30'},
                    {'name': 'Jane', 'age': '25'},
                ],
            )

    return _write


@pytest.fixture
def temp_json_file(
    tmp_path: Path,
) -> Callable[[JSONData], Path]:
    """
    Create a factory for writing a dictionary to a temporary JSON file and
    return its path.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory managed by pytest.

    Returns
    -------
    Callable[[JSONData], Path]
        Function that writes JSON data to a temporary JSON file and returns its
        path.
    """
    counter = itertools.count()

    def _write(data: JSONData, *, filename: str | None = None) -> Path:
        """Write JSON data to a temp file and return the resulting path."""
        name = filename or f'payload-{next(counter)}.json'
        path = tmp_path / name
        path.write_text(json.dumps(data, indent=2), encoding='utf-8')
        return path

    return _write
