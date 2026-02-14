"""
:mod:`tests.pytest_shared_support` module.

Shared typing protocols and helpers for top-level test fixtures.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from typing import Protocol

from requests import PreparedRequest  # type: ignore[import]

# SECTION: PROTOCOLS ======================================================== #


class CliInvoke(Protocol):
    """Protocol describing the :func:`cli_invoke` fixture."""

    def __call__(
        self,
        *cli_args: str | Sequence[str],
    ) -> tuple[int, str, str]: ...


class CliRunner(Protocol):
    """Protocol describing the ``cli_runner`` fixture."""

    def __call__(self, *cli_args: str | Sequence[str]) -> int: ...


class JsonFactory(Protocol):
    """Protocol describing the :func:`json_file_factory` fixture."""

    def __call__(
        self,
        payload: Any,
        *,
        filename: str | None = None,
        ensure_ascii: bool = False,
    ) -> Path: ...


class JsonOutputParser(Protocol):
    """Protocol for JSON parsing helpers."""

    def __call__(self, output: str | Path) -> Any: ...


class JsonFileParser(Protocol):
    """Protocol for JSON file parsing helpers."""

    def __call__(self, path: Path) -> Any: ...


class RequestFactory(Protocol):
    """Protocol describing prepared-request factories."""

    def __call__(
        self,
        url: str | None = None,
    ) -> PreparedRequest: ...


type CaptureHandler = Callable[[object, str], dict[str, object]]


def coerce_cli_args(
    cli_args: tuple[str | Sequence[str], ...],
) -> tuple[str, ...]:
    """Normalize CLI arguments into ``tuple[str, ...]``."""
    if (
        len(cli_args) == 1
        and isinstance(cli_args[0], Sequence)
        and not isinstance(cli_args[0], (str, bytes))
    ):
        return tuple(str(part) for part in cli_args[0])
    return tuple(str(part) for part in cli_args)


def parse_json(
    output: str | Path,
) -> Any:
    """Parse JSON from a string or file path."""
    raw = (
        output.read_text(encoding='utf-8')
        if isinstance(output, Path)
        else output
    )
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AssertionError(f'Expected JSON output, got: {raw!r}') from exc
