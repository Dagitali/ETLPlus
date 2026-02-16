"""
:mod:`tests.pytest_shared_fixtures` module.

Top-level shared pytest fixtures used across unit, smoke, and integration.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from etlplus.cli import main
from etlplus.types import JSONDict
from etlplus.types import JSONList

from .pytest_shared_support import CaptureHandler
from .pytest_shared_support import CliInvoke
from .pytest_shared_support import CliRunner
from .pytest_shared_support import JsonFactory
from .pytest_shared_support import JsonFileParser
from .pytest_shared_support import JsonOutputParser
from .pytest_shared_support import coerce_cli_args
from .pytest_shared_support import parse_json

# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='base_url')
def base_url_fixture() -> str:
    """Return the canonical base URL shared across tests."""
    return 'https://api.example.com'


@pytest.fixture(name='capture_handler')
def capture_handler_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> CaptureHandler:
    """Patch a handler function and capture the kwargs it receives."""

    def _capture(module: object, attr: str) -> dict[str, object]:
        calls: dict[str, object] = {}

        def _stub(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(module, attr, _stub)
        return calls

    return _capture


@pytest.fixture(name='json_file_factory')
def json_file_factory_fixture(
    tmp_path: Path,
) -> JsonFactory:
    """Create JSON files under *tmp_path* and return their paths."""

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
    name='sample_record',
    params=[
        pytest.param(
            {'id': 99, 'name': 'Grace'},
            id='single-record',
        ),
    ],
)
def sample_record_fixture(
    request: pytest.FixtureRequest,
) -> JSONDict:
    """Return a representative record payload for tests."""
    return request.param


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
) -> JSONList:
    """Return representative record payloads for tests."""
    return list(request.param)


@pytest.fixture(name='sample_records_json')
def sample_records_json_fixture(
    sample_records: JSONList,
) -> str:
    """Return sample records serialized as JSON."""
    return json.dumps(sample_records)


@pytest.fixture(name='json_payload_file')
def json_payload_file_fixture(
    json_file_factory: JsonFactory,
    sample_records: JSONList,
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
    """Parse JSON output emitted to stdout."""

    def _parse(output: str | Path) -> Any:
        return parse_json(output)

    return _parse


@pytest.fixture(name='parse_json_file')
def parse_json_file_fixture(
    parse_json_output: JsonOutputParser,
) -> JsonFileParser:
    """Parse JSON content from a file path."""

    def _parse(path: Path) -> Any:
        return parse_json_output(path)

    return _parse


@pytest.fixture(name='cli_runner')
def cli_runner_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> CliRunner:
    """Invoke CLI commands with isolated :mod:`sys.argv` state."""

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
    """Run CLI commands and return exit code, stdout, and stderr."""

    def _invoke(*cli_args: str | Sequence[str]) -> tuple[int, str, str]:
        exit_code = cli_runner(*cli_args)
        captured = capsys.readouterr()
        return exit_code, captured.out, captured.err

    return _invoke
