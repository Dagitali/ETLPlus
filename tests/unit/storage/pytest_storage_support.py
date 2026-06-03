"""
:mod:`tests.unit.storage.pytest_storage_support` module.

Shared test doubles for storage unit tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# SECTION: CLASSES ========================================================== #


class FakeHttpResponse:
    """Minimal HTTP response test double."""

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        *,
        status_code: int,
        payload: bytes = b'',
    ) -> None:
        self.status_code = status_code
        self.content = payload

    # -- Instance Methods -- #

    def close(self) -> None:
        """Close the response without side effects."""

    def raise_for_status(self) -> None:
        """Raise one error for non-successful response codes."""
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class FakeHttpSession:
    """Minimal requests-session test double for HTTP storage tests."""

    # -- Magic Methods (Object Lifecycle) -- #

    def __init__(
        self,
        *,
        head_status: int = 200,
        get_status: int = 200,
        payload: bytes = b'',
    ) -> None:
        self.calls: list[tuple[str, str, bool]] = []
        self.head_status = head_status
        self.get_status = get_status
        self.payload = payload

    # -- Instance Methods -- #

    def close(self) -> None:
        """Close the fake session without side effects."""

    def get(self, url: str, **kwargs: Any) -> FakeHttpResponse:
        """Return one fake GET response and capture call metadata."""
        self.calls.append(('get', url, bool(kwargs.get('stream', False))))
        return FakeHttpResponse(
            status_code=self.get_status,
            payload=self.payload,
        )

    def head(self, url: str, **kwargs: Any) -> FakeHttpResponse:
        """Return one fake HEAD response and capture call metadata."""
        self.calls.append(
            ('head', url, bool(kwargs.get('allow_redirects', False))),
        )
        return FakeHttpResponse(status_code=self.head_status)


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class FakeContentSettings:
    """Minimal content-settings test double."""

    content_type: str
