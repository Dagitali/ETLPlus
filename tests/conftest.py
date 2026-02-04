"""
:mod:`tests.conftest` module.

Define shared fixtures and helpers for pytest-based tests of :mod:`etlplus`.

Notes
-----
- Provides CLI helpers so tests no longer need to monkeypatch :mod:`sys.argv`
    inline.
- Supplies JSON file factories that rely on ``tmp_path`` for automatic
    cleanup.
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

# SECTION: TYPES ============================================================ #


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


# SECTION: FUNCTIONS ======================================================== #


def coerce_cli_args(
    cli_args: tuple[str | Sequence[str], ...],
) -> tuple[str, ...]:
    """
    Normalize CLI arguments into a ``tuple[str, ...]``.

    Parameters
    ----------
    cli_args : tuple[str | Sequence[str], ...]
        Arguments provided to CLI helpers.

    Returns
    -------
    tuple[str, ...]
        Normalized argument tuple safe to concatenate with :mod:`sys.argv`.
    """
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
    """
    Parse JSON from a string or file path.

    Parameters
    ----------
    output : str | Path
        JSON string or file path.

    Returns
    -------
    Any
        Parsed JSON payload.

    Raises
    ------
    AssertionError
        If the payload is not valid JSON.
    """
    raw = (
        output.read_text(encoding='utf-8')
        if isinstance(output, Path)
        else output
    )
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AssertionError(f'Expected JSON output, got: {raw!r}') from exc


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


@pytest.fixture(
    name='sample_records',
    params=[
        pytest.param(
            [
                {'id': 1, 'name': 'Alice'},
                {'id': 2, 'name': 'Bob'},
            ],
            id='two-records',
        ),
        pytest.param(
            [
                {'id': 99, 'name': 'Grace'},
            ],
            id='single-record',
        ),
    ],
)
def sample_records_fixture(
    request: pytest.FixtureRequest,
) -> list[dict[str, Any]]:
    """
    Return representative record payloads for tests.

    Parameters
    ----------
    request : pytest.FixtureRequest
        Pytest fixture request carrying the parametrized payload.

    Returns
    -------
    list[dict[str, Any]]
        The sample record payload.
    """
    return list(request.param)


@pytest.fixture(name='sample_records_json')
def sample_records_json_fixture(
    sample_records: list[dict[str, Any]],
) -> str:
    """Return sample records serialized as JSON."""
    return json.dumps(sample_records)


@pytest.fixture(name='json_payload_file')
def json_payload_file_fixture(
    json_file_factory: JsonFactory,
    sample_records: list[dict[str, Any]],
) -> Path:
    """Persist ``sample_records`` as JSON and return the file path."""
    return json_file_factory(sample_records, filename='records.json')


@pytest.fixture(name='rules_json')
def rules_json_fixture() -> str:
    """Return simple validation rules as a JSON string."""
    rules = {
        'id': {'type': 'integer', 'min': 0},
        'name': {'type': 'string', 'minLength': 1},
    }
    return json.dumps(rules)


@pytest.fixture(name='operations_json')
def operations_json_fixture() -> str:
    """Return a basic transform operation payload as JSON."""
    return json.dumps({'select': ['id']})


@pytest.fixture(name='parse_json_output')
def parse_json_output_fixture() -> JsonOutputParser:
    """
    Parse JSON output emitted to stdout.

    Returns
    -------
    JsonOutputParser
        Callable that parses JSON strings and raises AssertionError on
        malformed output.
    """

    def _parse(output: str | Path) -> Any:
        return parse_json(output)

    return _parse


@pytest.fixture(name='parse_json_file')
def parse_json_file_fixture(
    parse_json_output: JsonOutputParser,
) -> JsonFileParser:
    """
    Parse JSON content from a file path.

    Parameters
    ----------
    parse_json_output : JsonOutputParser
        Shared JSON parsing helper.

    Returns
    -------
    JsonFileParser
        Callable that parses JSON from a file path.
    """

    def _parse(path: Path) -> Any:
        return parse_json_output(path)

    return _parse


@pytest.fixture(name='cli_runner')
def cli_runner_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> CliRunner:
    """
    Invoke ``etlplus`` CLI commands with isolated :mod:`sys.argv` state.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Built-in pytest fixture used to patch :mod:`sys.argv`.

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
        args = coerce_cli_args(cli_args)
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
