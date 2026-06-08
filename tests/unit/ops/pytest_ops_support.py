"""
:mod:`tests.unit.ops.pytest_ops_support` module.

Shared :mod:`etlplus.ops` unit-test helpers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# SECTION: FUNCTIONS ======================================================= #


def write_json_payload(
    path: str | Path,
    payload: object,
) -> None:
    """Write one JSON payload using UTF-8 encoding."""
    Path(path).write_text(json.dumps(payload), encoding='utf-8')


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class ApiCallRecord:
    """Record of one HTTP method call in an API session test double."""

    method: str
    url: str
    json: object
    timeout: float
    kwargs: dict[str, Any]


# SECTION: CLASSES ========================================================== #


class ApiSession:
    """Capture POST and PUT calls for API load tests."""

    def __init__(self, payload: object | None = None) -> None:
        self.calls: list[ApiCallRecord] = []
        self.payload = payload or {'ok': True}

    def _record(
        self,
        method: str,
        url: str,
        *,
        json_payload: object,
        timeout: float,
        kwargs: dict[str, Any],
    ) -> JsonResponse:
        """Capture one API request and return a JSON response."""
        self.calls.append(
            ApiCallRecord(
                method=method,
                url=url,
                json=json_payload,
                timeout=timeout,
                kwargs=dict(kwargs),
            ),
        )
        return JsonResponse(self.payload, text='ok')

    def post(
        self,
        url: str,
        *,
        timeout: float,
        **kwargs: Any,
    ) -> JsonResponse:
        """Capture POST call details."""
        json_payload = kwargs.pop('json')
        return self._record(
            'post', url, json_payload=json_payload, timeout=timeout, kwargs=kwargs,
        )

    def put(
        self,
        url: str,
        *,
        timeout: float,
        **kwargs: Any,
    ) -> JsonResponse:
        """Capture PUT call details."""
        json_payload = kwargs.pop('json')
        return self._record(
            'put', url, json_payload=json_payload, timeout=timeout, kwargs=kwargs,
        )


class JsonResponse:
    """Configurable response test double for ops HTTP tests."""

    def __init__(
        self,
        payload: object | None = None,
        *,
        headers: dict[str, str] | None = None,
        status_code: int = 200,
        text: str = 'fallback',
        json_error: bool = False,
    ) -> None:
        self.headers = headers or {}
        self.status_code = status_code
        self.text = text
        self._payload = {'ok': True} if payload is None else payload
        self._json_error = json_error

    def json(self) -> object:
        """Return the pre-set payload or raise JSON error."""
        if self._json_error:
            raise ValueError('bad json')
        return self._payload

    def raise_for_status(self) -> None:
        """No-op status check for request execution tests."""
        return


class MethodSession:
    """Lightweight session exposing one configured HTTP method."""

    def __init__(
        self,
        response: JsonResponse,
        *,
        method_name: str = 'get',
    ) -> None:
        self._response = response
        self.calls: list[dict[str, Any]] = []
        setattr(self, method_name, self._make_call)

    def _make_call(
        self,
        url: str,
        **kwargs: Any,
    ) -> JsonResponse:
        """Record the call and return the pre-set response."""
        self.calls.append({'url': url, 'kwargs': kwargs})
        return self._response
