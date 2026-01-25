"""
:mod:`tests.unit.conftest` module.

Configures pytest-based unit tests and provides shared fixtures.

Notes
-----
- Fixtures are designed for reuse and DRY test setup.
"""

from __future__ import annotations

import csv
import itertools
import json
import random
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any
from typing import TypedDict
from typing import Unpack
from typing import cast

import pytest
import requests  # type: ignore[import]

import etlplus.api.rate_limiting.rate_limiter as rl_module
from etlplus.api import ApiConfig
from etlplus.api import ApiProfileConfig
from etlplus.api import CursorPaginationConfigMap
from etlplus.api import EndpointClient
from etlplus.api import EndpointConfig
from etlplus.api import PagePaginationConfigMap
from etlplus.api import PaginationConfig
from etlplus.api import PaginationConfigMap
from etlplus.api import RateLimitConfig
from etlplus.api import RateLimitConfigMap
from etlplus.types import JSONData
from etlplus.workflow import PipelineConfig
from tests.unit.api.test_u_mocks import MockSession

# SECTION: HELPERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


class _CursorKw(TypedDict, total=False):
    cursor_param: str
    cursor_path: str
    page_size: int | str
    records_path: str
    start_cursor: str | int
    max_pages: int
    max_records: int


class _PageKw(TypedDict, total=False):
    page_param: str
    size_param: str
    start_page: int
    page_size: int
    records_path: str
    max_pages: int
    max_records: int


def _freeze(
    d: dict[str, Any],
) -> types.MappingProxyType:
    """
    Create an immutable, read-only mapping proxy for a dictionary.

    Parameters
    ----------
    d : dict[str, Any]
        Dictionary to freeze.

    Returns
    -------
    types.MappingProxyType
        Read-only mapping proxy of the input dictionary.
    """
    return types.MappingProxyType(d)


# SECTION: FIXTURES (API) =================================================== #


@pytest.fixture
def api_profile_defaults_factory() -> Callable[..., dict[str, Any]]:
    """
    Create a factory to build API profile defaults block dictionaries.

    Returns
    -------
    Callable[..., dict[str, Any]]
        Function that builds a profile defaults mapping for API config.

    Examples
    --------
    >>> defaults = api_profile_defaults_factory(
    ...     pagination={'type': 'page', 'page_param': 'p', 'size_param': 's'},
    ...     rate_limit={'sleep_seconds': 0.1, 'max_per_sec': 5},
    ...     headers={'X': '1'},
    ... )
    """

    def _make(
        *,
        pagination: dict[str, Any] | None = None,
        rate_limit: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if pagination is not None:
            out['pagination'] = pagination
        if rate_limit is not None:
            out['rate_limit'] = rate_limit
        if headers is not None:
            out['headers'] = headers
        return out

    return _make


@pytest.fixture
def capture_sleeps(
    monkeypatch: pytest.MonkeyPatch,
) -> list[float]:
    """
    Capture sleep durations from retry/backoff logic.

    Patches :class:`RateLimiter` so tests can assert jitter/backoff behavior
    without actually waiting.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.

    Returns
    -------
    list[float]
        List of sleep durations applied during test execution.
    """
    values: list[float] = []

    def _enforce(self: rl_module.RateLimiter) -> None:  # noqa: D401
        values.append(self.sleep_seconds)

    monkeypatch.setattr(
        rl_module.RateLimiter,
        'enforce',
        _enforce,
        raising=False,
    )

    return values


@pytest.fixture
def client_factory(
    base_url: str,
) -> Callable[..., EndpointClient]:
    """
    Create a factory to build :class:`EndpointClient` instances.

    Parameters can be overridden per test. Endpoints default to an empty
    mapping for convenience.

    Parameters
    ----------
    base_url : str
        Common base URL used across tests.

    Returns
    -------
    Callable[..., EndpointClient]
        Function that builds :class:`EndpointClient` instances.
    """

    def _make(
        *,
        base_url: str = base_url,
        endpoints: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> EndpointClient:
        return EndpointClient(
            base_url=base_url,
            endpoints=endpoints or {},
            **kwargs,
        )

    return _make


@pytest.fixture
def cursor_cfg() -> Callable[..., CursorPaginationConfigMap]:
    """
    Create a factory for building immutable cursor pagination config objects.

    Returns
    -------
    Callable[..., CursorPaginationConfigMap]
        Function that builds :class:`CursorPaginationConfigMap` instances.
    """

    def _make(**kwargs: Unpack[_CursorKw]) -> CursorPaginationConfigMap:
        base: dict[str, Any] = {'type': 'cursor'}
        base.update(kwargs)
        return cast(CursorPaginationConfigMap, _freeze(base))

    return _make


@pytest.fixture
def offset_cfg() -> Callable[..., PagePaginationConfigMap]:
    """
    Create a factory for building immutable offset pagination config objects.

    Returns
    -------
    Callable[..., PagePaginationConfigMap]
        Function that builds PagePaginationConfigMap instances.
    """

    def _make(**kwargs: Unpack[_PageKw]) -> PagePaginationConfigMap:
        base: dict[str, Any] = {'type': 'offset'}
        base.update(kwargs)
        return cast(PagePaginationConfigMap, _freeze(base))

    return _make


@pytest.fixture
def request_once_stub(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    """
    Patch :meth:`EndpointClient.request_once` and capture calls.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture for patching.

    Returns
    -------
    dict[str, Any]
        Dictionary with:
            urls: list[str]
            kwargs: list[dict[str, Any]]
    """
    # pylint: disable=unused-argument

    # Locally import to avoid cycles.
    import etlplus.api.request_manager as rm_module

    calls: dict[str, Any] = {'urls': [], 'kwargs': []}

    def _fake_request(
        self: rm_module.RequestManager,
        method: str,
        url: str,
        *,
        session: Any,
        timeout: Any,
        request_callable: Callable[..., Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:  # noqa: D401
        assert method == 'GET'
        calls['urls'].append(url)
        calls['kwargs'].append(kwargs)
        return {'ok': True}

    monkeypatch.setattr(
        rm_module.RequestManager,
        'request_once',
        _fake_request,
    )

    return calls


@pytest.fixture(scope='session')
def extract_stub_factory() -> Callable[..., Any]:
    """
    Create a factory to build a per-use stub factory for patching
    the low-level HTTP helper without relying on function-scoped fixtures
    (Hypothesis-friendly).

    Each invocation patches
    :meth:`etlplus.api.endpoint_client.EndpointClient.request_once` for the
    duration of the context manager and restores the original
    afterwards.

    Returns
    -------
    Callable[..., Any]
        Function that builds a call capture dictionary.

    Examples
    --------
    >>> with extract_stub_factory() as calls:
    ...     client.paginate(...)
    ...     assert calls['urls'] == [...]
    """
    # pylint: disable=unused-argument

    import contextlib

    # Locally import to avoid cycles.
    import etlplus.api.request_manager as rm_module

    @contextlib.contextmanager
    def _make(
        *,
        return_value: Any | None = None,
    ):  # noqa: D401
        # pylint: disable=protected-access

        calls: dict[str, Any] = {'urls': [], 'kwargs': []}

        def _fake_request(
            self: rm_module.RequestManager,
            method: str,
            url: str,
            *,
            session: Any,
            timeout: Any,
            request_callable: Callable[..., Any] | None = None,
            **kwargs: Any,
        ) -> dict[str, Any] | list[dict[str, Any]]:  # noqa: D401
            calls['urls'].append(url)
            calls['kwargs'].append(kwargs)
            return {'ok': True} if return_value is None else return_value

        saved = rm_module.RequestManager.request_once
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(
            rm_module.RequestManager,
            'request_once',
            _fake_request,
        )
        try:
            yield calls
        finally:
            monkeypatch.setattr(
                rm_module.RequestManager,
                'request_once',
                saved,
            )

    return _make


@pytest.fixture
def jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[list[float]], list[float]]:
    """
    Set retry jitter sequence deterministically.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.

    Returns
    -------
    Callable[[list[float]], list[float]]
        Function that sets the sequence of jitter values for random.uniform.

    Examples
    --------
    >>> vals = jitter([0.1, 0.2])
    ... # Now client jitter will use 0.1, then 0.2 for random.uniform(a, b)
    """

    def _set(values: list[float]) -> list[float]:
        seq = iter(values)
        monkeypatch.setattr(random, 'uniform', lambda a, b: next(seq))
        return values

    return _set


@pytest.fixture
def mock_session() -> MockSession:
    """
    Provide a fresh :class:`MockSession` per test.

    Use for tests that need to pass a raw session into :class:`EndpointClient`
    or verify close semantics.

    Returns
    -------
    MockSession
        New :class:`MockSession` instance.
    """
    return MockSession()


@pytest.fixture
def page_cfg() -> Callable[..., PagePaginationConfigMap]:
    """
    Create a factory to build immutable page-number pagination config objects.

    Returns
    -------
    Callable[..., PagePaginationConfigMap]
        Function that builds :class:`PagePaginationConfigMap` instances.
    """

    def _make(**kwargs: Unpack[_PageKw]) -> PagePaginationConfigMap:
        base: dict[str, Any] = {'type': 'page'}
        base.update(kwargs)
        return cast(PagePaginationConfigMap, _freeze(base))

    return _make


@pytest.fixture
def retry_cfg() -> Callable[..., dict[str, Any]]:
    """
    Create a factory to build retry configuration dictionaries for
    :class:`EndpointClient`.

    Returns
    -------
    Callable[..., dict[str, Any]]
        Function that builds retry configuration dicts for
        :class:`EndpointClient`.
    """

    def _make(**kwargs: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            'max_attempts': kwargs.pop('max_attempts', 3),
            'backoff': kwargs.pop('backoff', 0.0),
        }
        base.update(kwargs)
        return base

    return _make


@pytest.fixture
def token_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, int]:
    """
    Track token fetch count and patch requests.post for token acquisition.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.

    Returns
    -------
    dict[str, int]
        Dictionary tracking token fetch count.
    """
    # pylint: disable=unused-argument

    calls: dict[str, int] = {'n': 0}

    def fake_post(
        *args,
        **kwargs,
    ) -> object:
        calls['n'] += 1
        # _Resp is defined in test_u_auth.py, so return a dict for generality.
        return {'access_token': f't{calls["n"]}', 'expires_in': 60}

    monkeypatch.setattr(requests, 'post', fake_post)

    return calls


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
    sample_endpoints_: dict[str, dict[str, Any]],
) -> Callable[..., dict[str, Any]]:
    """
    Create a factory for building API configuration dicts for
    :meth:`ApiConfig.from_obj`.

    Parameters
    ----------
    base_url : str
        Common base URL used across config tests.
    sample_endpoints_ : dict[str, dict[str, Any]]
        Common endpoints mapping for config tests.

    Returns
    -------
    Callable[..., dict[str, Any]]
        Function that builds API configuration dicts for ApiConfig.

    Examples
    --------
    >>> obj = api_obj_factory(base_path='/v1', headers={'X': '1'})
    ... cfg = ApiConfig.from_obj(obj)
    """

    def _make(
        *,
        use_profiles: bool | None = False,
        base_path: str | None = None,
        headers: dict[str, str] | None = None,
        endpoints: dict[str, dict[str, Any]] | None = None,
        defaults: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        eps = endpoints or sample_endpoints_
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

    def _make(obj: PaginationConfigMap) -> PaginationConfig:  # noqa: ANN401
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
def pipeline_from_yaml_factory() -> Callable[..., PipelineConfig]:
    """
    Create a factory to build :class:`PipelineConfig` from a YAML file path.

    Returns
    -------
    Callable[..., PipelineConfig]
        Function that builds :class:`PipelineConfig` from a YAML file.
    """

    def _make(
        path: Path,
        *,
        substitute: bool = True,
        env: dict[str, str] | None = None,
    ) -> PipelineConfig:
        return PipelineConfig.from_yaml(
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
    [RateLimitConfigMap],
    RateLimitConfig,
]:
    """
    Create a factory to build :class:`RateLimitConfig` via `from_obj` mapping.

    Returns
    -------
    Callable[[RateLimitConfigMap], RateLimitConfig]
        Function that builds :class:`RateLimitConfig` from mapping.
    """

    def _make(obj: RateLimitConfigMap) -> RateLimitConfig:
        return RateLimitConfig.from_obj(obj)

    return _make


@pytest.fixture(name='sample_endpoints_')
def sample_endpoints_fixture() -> dict[str, dict[str, Any]]:
    """
    Return a common endpoints mapping for config tests.

    Returns
    -------
    dict[str, dict[str, Any]]
        Dictionary of endpoint mappings.
    """
    return {
        'users': {'path': '/users'},
        'list': {'path': '/items'},
        'ping': {'path': '/ping'},
    }


@pytest.fixture
def sample_headers() -> dict[str, str]:
    """
    Return a common headers mapping for config tests.

    Returns
    -------
    dict[str, str]
        Dictionary of common headers.
    """
    return {'Accept': 'application/json'}


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
