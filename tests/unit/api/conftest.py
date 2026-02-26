"""
:mod:`tests.unit.api.conftest` module.

Shared fixtures and helpers for pytest-based unit tests of
:mod:`etlplus.api` modules.

Notes
-----
- Fixtures are designed for reuse and DRY test setup.
"""

from __future__ import annotations

import types
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import cast

import pytest
import requests  # type: ignore[import]

import etlplus.api.rate_limiting.rate_limiter as rl_module
import etlplus.api.retry_manager as rm_module

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: TYPE ALIASES ===================================================== #


type FakeResponseFactory = Callable[..., '_FakeResponse']
type FakeHttpErrorFactory = Callable[[int, str], requests.HTTPError]
type FakeRequestErrorFactory = Callable[
    [str],
    requests.RequestException,
]
type AuthPostErrorCase = tuple[Callable[..., Any], type[Exception]]
type AuthRequestExceptionCase = tuple[
    requests.RequestException,
    type[Exception],
]
type PaginatorModeCase = tuple[str, int | None, int]


# SECTION: HELPERS ========================================================== #


@dataclass(slots=True)
class _FakeResponse:
    """Lightweight response stub with ``requests.Response``-like behavior."""

    payload: Any = None
    status_code: int = 200
    text: str = ''
    content_type: str = 'application/json'
    json_raises: bool = False
    headers: dict[str, str] = field(init=False)

    def __post_init__(self) -> None:
        self.headers = {'content-type': self.content_type}
        if not self.text:
            self.text = str(self.payload)

    def raise_for_status(self) -> None:
        """Raise ``HTTPError`` for non-success statuses."""
        if self.status_code >= 400:
            err = requests.HTTPError(f'HTTP {self.status_code}')
            err.response = cast(
                requests.Response,
                types.SimpleNamespace(
                    status_code=self.status_code,
                    text=self.text,
                ),
            )
            raise err

    def json(self) -> Any:
        """Return payload or raise ``ValueError`` when configured."""
        if self.json_raises:
            raise ValueError('invalid json')
        return self.payload


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(
    name='auth_post_error_case',
    params=[
        pytest.param('http-error', id='http-error'),
        pytest.param('missing-token', id='missing-token'),
        pytest.param('invalid-json', id='invalid-json'),
    ],
)
def auth_post_error_case_fixture(
    request: pytest.FixtureRequest,
    fake_response_factory: FakeResponseFactory,
) -> AuthPostErrorCase:
    """Matrix of token POST failure branches."""
    case = cast(str, request.param)
    if case == 'http-error':
        return (
            lambda *_a, **_k: fake_response_factory(payload={}, status=401),
            requests.HTTPError,
        )
    if case == 'missing-token':
        return (
            lambda *_a, **_k: fake_response_factory(
                payload={'expires_in': 60},
            ),
            RuntimeError,
        )
    return (
        lambda *_a, **_k: fake_response_factory(
            payload=None,
            text='not json',
            json_raises=True,
        ),
        ValueError,
    )


@pytest.fixture(
    name='auth_request_exception_case',
    params=[
        pytest.param('timeout', id='timeout'),
        pytest.param('ssl', id='ssl'),
        pytest.param('connection', id='connection'),
        pytest.param('request', id='request-exception'),
    ],
)
def auth_request_exception_case_fixture(
    request: pytest.FixtureRequest,
    fake_request_error_factory: FakeRequestErrorFactory,
) -> AuthRequestExceptionCase:
    """Matrix of request exception branches in token exchange."""
    kind = cast(str, request.param)
    exc = fake_request_error_factory(kind)
    expected = type(exc)
    return exc, expected


@pytest.fixture
def capture_sleeps(
    monkeypatch: pytest.MonkeyPatch,
) -> list[float]:
    """
    Capture sleep durations from retries and rate limiting.

    Patches :class:`RetryManager` so that its ``sleeper`` callable appends
    sleep durations to a list instead of actually sleeping. Also patches
    :class:`RateLimiter` to record rate-limit sleeps into the same list.
    """
    sleeps: list[float] = []

    # Patch RetryManager to inject a recording sleeper when none is given.
    original_init = rm_module.RetryManager.__init__

    def _init(self, *args, **kwargs):
        if 'sleeper' not in kwargs:

            def _sleeper(seconds: float) -> None:
                sleeps.append(seconds)

            kwargs['sleeper'] = _sleeper
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(
        rm_module.RetryManager,
        '__init__',
        _init,  # type: ignore[assignment]
    )

    # Patch :meth:`RateLimiter.enforce` so rate-limit sleeps are captured.
    def _capture_sleep(self: rl_module.RateLimiter) -> None:
        sleeps.append(self.sleep_seconds)

    monkeypatch.setattr(
        rl_module.RateLimiter,
        'enforce',
        _capture_sleep,
    )

    return sleeps


@pytest.fixture(name='fake_response_factory')
def fake_response_factory_fixture() -> FakeResponseFactory:
    """
    Build fake response objects that mimic the small subset tests need.
    """

    def _make(
        *,
        payload: Any = None,
        status: int = 200,
        text: str = '',
        content_type: str = 'application/json',
        json_raises: bool = False,
    ) -> _FakeResponse:
        return _FakeResponse(
            payload=payload,
            status_code=status,
            text=text,
            content_type=content_type,
            json_raises=json_raises,
        )

    return _make


@pytest.fixture(name='fake_http_error_factory')
def fake_http_error_factory_fixture() -> FakeHttpErrorFactory:
    """Create ``requests.HTTPError`` instances with attached responses."""

    def _make(
        status: int,
        text: str = 'boom',
    ) -> requests.HTTPError:
        err = requests.HTTPError(f'HTTP {status}')
        err.response = cast(
            requests.Response,
            types.SimpleNamespace(status_code=status, text=text),
        )
        return err

    return _make


@pytest.fixture(name='fake_request_error_factory')
def fake_request_error_factory_fixture(
    fake_http_error_factory: FakeHttpErrorFactory,
) -> FakeRequestErrorFactory:
    """Create request exceptions by semantic kind."""

    def _make(kind: str) -> requests.RequestException:
        match kind:
            case 'http':
                return fake_http_error_factory(500, 'boom')
            case 'timeout':
                return requests.exceptions.Timeout('timeout')
            case 'ssl':
                return requests.exceptions.SSLError('ssl')
            case 'connection':
                return requests.exceptions.ConnectionError('connection')
            case _:
                return requests.exceptions.RequestException('request')

    return _make


@pytest.fixture
def jitter(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[list[float]], list[float]]:
    """
    Configure deterministic jitter values for retry backoff.

    Returns a callable that, when invoked with a list of floats, seeds the
    sequence of values returned by :func:`random.uniform`.


    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.

    Returns
    -------
    Callable[[list[float]], list[float]]
        Function that sets the sequence of jitter values for
        ``RetryManager.random.uniform``.

    Examples
    --------
    >>> vals = jitter([0.1, 0.2])
    ... # Now retry jitter will use 0.1, then 0.2 for uniform(a, b)
    """
    values: list[float] = []

    def set_values(new_values: list[float]) -> list[float]:
        values.clear()
        values.extend(new_values)
        return values

    def fake_uniform(_a: float, b: float) -> float:
        if values:
            return values.pop(0)
        return b

    monkeypatch.setattr(
        rm_module.random,
        'uniform',
        fake_uniform,
    )
    return set_values


@pytest.fixture(
    name='paginator_mode_case',
    params=[
        pytest.param(('page', None, 1), id='page-none'),
        pytest.param(('page', -5, 1), id='page-negative'),
        pytest.param(('page', 0, 1), id='page-zero'),
        pytest.param(('page', 3, 3), id='page-custom'),
        pytest.param(('offset', None, 0), id='offset-none'),
        pytest.param(('offset', -5, 0), id='offset-negative'),
        pytest.param(('offset', 0, 0), id='offset-zero'),
        pytest.param(('offset', 10, 10), id='offset-custom'),
        pytest.param(('bogus', 7, 7), id='bogus-falls-back'),
    ],
)
def paginator_mode_case_fixture(
    request: pytest.FixtureRequest,
) -> PaginatorModeCase:
    """Matrix of paginator type/start-page normalization scenarios."""
    return cast(PaginatorModeCase, request.param)


@pytest.fixture(name='patch_sleep', autouse=True)
def patch_sleep_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Disable sleeping during tests to keep the suite fast.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Built-in pytest fixture used to patch attributes.
    """
    # Patch the module-level sleep helper so :class:`RateLimiter` continues to
    # invoke ``time.sleep`` (allowing targeted tests to inspect it) without
    # pausing.
    monkeypatch.setattr(
        rl_module.time,
        'sleep',
        lambda _seconds: None,
    )
