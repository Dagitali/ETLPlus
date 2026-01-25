"""
:mod:`tests.conftest` module.

Global pytest fixtures shared across unit, integration, and end-to-end tests.

Notes
-----
- Provides CLI helpers so tests no longer need to monkeypatch ``sys.argv``
    inline.
- Supplies JSON file factories that rely on ``tmp_path`` for automatic
    cleanup.
- Keeps docstrings NumPy-formatted to satisfy numpydoc linting.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from typing import Protocol

import pytest
from requests import PreparedRequest  # type: ignore[import]

from etlplus.cli import main

# SECTION: HELPERS ========================================================== #


def _coerce_cli_args(
    cli_args: tuple[str | Sequence[str], ...],
) -> tuple[str, ...]:
    """
    Normalize CLI arguments into a ``tuple[str, ...]``.

    Parameters
    ----------
    cli_args : tuple[str | Sequence[str], ...]
        Arguments provided to ``cli_runner``/``cli_invoke``.

    Returns
    -------
    tuple[str, ...]
        Normalized argument tuple safe to concatenate with ``sys.argv``.
    """
    if (
        len(cli_args) == 1
        and isinstance(cli_args[0], Sequence)
        and not isinstance(cli_args[0], (str, bytes))
    ):
        return tuple(str(part) for part in cli_args[0])
    return tuple(str(part) for part in cli_args)


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


class RequestFactory(Protocol):
    """Protocol describing prepared-request factories."""

    def __call__(
        self,
        url: str | None = None,
    ) -> PreparedRequest: ...


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='base_url')
def base_url_fixture() -> str:
    """Return the canonical base URL shared across tests."""

    return 'https://api.example.com'


@pytest.fixture(name='json_file_factory')
def json_file_factory_fixture(
    tmp_path: Path,
) -> JsonFactory:
    """
    Create JSON files under *tmp_path* and return their paths.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory managed by pytest.

    Returns
    -------
    JsonFactory
        Factory that persists the provided payload as JSON and returns the
        resulting path.

    Examples
    --------
    >>> path = json_file_factory({'name': 'Ada'})
    >>> path.exists()
    True
    """

    def _create(
        payload: Any,
        *,
        filename: str | None = None,
        ensure_ascii: bool = False,
    ) -> Path:
        target = tmp_path / (filename or 'payload.json')
        data = (
            payload
            if isinstance(payload, str)
            else json.dumps(payload, indent=2, ensure_ascii=ensure_ascii)
        )
        target.write_text(data)
        return target

    return _create


@pytest.fixture(name='cli_runner')
def cli_runner_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> CliRunner:
    """
    Invoke ``etlplus`` CLI commands with isolated ``sys.argv`` state.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Built-in pytest fixture used to patch ``sys.argv``.

    Returns
    -------
    CliRunner
        Helper that accepts CLI arguments, runs :func:`etlplus.cli.main`, and
        returns the exit code.

    Examples
    --------
    >>> cli_runner(('extract', 'file', 'data.json'))
    0
    """

    def _run(*cli_args: str | Sequence[str]) -> int:
        args = _coerce_cli_args(cli_args)
        monkeypatch.setattr(sys, 'argv', ['etlplus', *args])
        return main()

    return _run


@pytest.fixture
def cli_invoke(
    cli_runner: CliRunner,
    capsys: pytest.CaptureFixture[str],
) -> CliInvoke:
    """
    Run CLI commands and return exit code, STDOUT, and stderr.

    Parameters
    ----------
    cli_runner : CliRunner
        Helper fixture defined above.
    capsys : pytest.CaptureFixture[str]
        Pytest fixture for capturing STDOUT/stderr.

    Returns
    -------
    CliInvoke
        Helper that yields ``(exit_code, stdout, stderr)`` tuples.

    Examples
    --------
    >>> code, out, err = cli_invoke(('extract', 'file', 'data.json'))
    >>> code
    0
    """

    def _invoke(*cli_args: str | Sequence[str]) -> tuple[int, str, str]:
        exit_code = cli_runner(*cli_args)
        captured = capsys.readouterr()
        return exit_code, captured.out, captured.err

    return _invoke
