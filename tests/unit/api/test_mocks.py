from __future__ import annotations

from typing import Any

import requests  # type: ignore
from requests.structures import CaseInsensitiveDict  # type: ignore[import]


class MockResponse(requests.Response):
    """Minimal Response subclass that returns a provided JSON payload.

    Subclassing ``requests.Response`` keeps the return type compatible for
    test double usage while overriding ``json`` for simplicity.
    """

    def __init__(self, payload: Any) -> None:  # pragma: no cover - trivial
        super().__init__()
        self._payload = payload
        self.status_code = 200
        self.headers = CaseInsensitiveDict({
            'content-type': 'application/json',
        })

    def json(self, **_kw: Any) -> Any:  # pragma: no cover - trivial
        return self._payload


class MockSession(requests.Session):
    """Session test double capturing GET calls.

    Inherits from ``requests.Session`` so that it is type-compatible with
    the ``EndpointClient`` constructor signature (``requests.Session | None``),
    fixing mypy ``arg-type`` errors in tests. Only the ``get`` and ``close``
    methods are customized; other behavior defers to the base class.
    """

    def __init__(self) -> None:  # pragma: no cover - trivial
        super().__init__()
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.closed: bool = False

    def get(  # type: ignore[override]
        self,
        url: str,
        params: Any = None,
        **kwargs: Any,
    ) -> requests.Response:
        # Normalize kwargs incl. params for assertions.
        call_kwargs: dict[str, Any] = dict(kwargs)
        if params is not None:
            call_kwargs['params'] = params
        self.calls.append((url, call_kwargs))
        return MockResponse({'ok': True})

    def close(self) -> None:  # pragma: no cover - trivial
        super().close()
        self.closed = True
