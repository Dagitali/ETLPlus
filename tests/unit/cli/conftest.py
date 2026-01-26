"""
:mod:`tests.unit.cli.conftest` module.

Configures pytest-based unit tests and provides shared fixtures for
:mod:`etlplus.cli` unit tests.

Notes
-----
- Fixtures are designed for reuse and DRY test setup.
"""

from __future__ import annotations

import json
import types
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Final
from typing import cast

import pytest
import typer
from click.testing import Result
from typer.testing import CliRunner

from etlplus.cli.commands import app as cli_app
from etlplus.workflow import PipelineConfig

# SECTION: HELPERS ======================================================== #


CSV_TEXT: Final[str] = 'a,b\n1,2\n3,4\n'

type CaptureHandler = Callable[[object, str], dict[str, object]]
type CaptureIo = dict[str, list[tuple[tuple[object, ...], dict[str, object]]]]
type InvokeCli = Callable[..., Result]
type StubCommand = Callable[[Callable[..., object]], None]


def assert_emit_json(
    calls: CaptureIo,
    payload: object,
    *,
    pretty: bool,
) -> None:
    """
    Assert that :func:`emit_json` was called once with expected payload.

    Parameters
    ----------
    calls : CaptureIo
        Captured IO calls from the CLI fixtures.
    payload : object
        Expected JSON payload.
    pretty : bool
        Expected pretty-print flag.
    """
    assert calls['emit_json'] == [((payload,), {'pretty': pretty})]


def assert_emit_or_write(
    calls: CaptureIo,
    payload: object,
    target: object,
    *,
    pretty: bool,
) -> dict[str, object]:
    """
    Assert that :func:`emit_or_write` was called once and return kwargs.

    Parameters
    ----------
    calls : CaptureIo
        Captured IO calls from the CLI fixtures.
    payload : object
        Expected payload argument.
    target : object
        Expected output path argument.
    pretty : bool
        Expected pretty-print flag.

    Returns
    -------
    dict[str, object]
        Captured keyword arguments for the call.
    """
    assert len(calls['emit_or_write']) == 1
    args, kwargs = calls['emit_or_write'][0]
    assert args[0] == payload
    assert args[1] == target
    assert kwargs['pretty'] is pretty
    return kwargs


def assert_mapping_contains(
    actual: Mapping[str, object],
    expected: Mapping[str, object],
) -> None:
    """
    Assert that *actual* contains the *expected* key/value pairs.

    Parameters
    ----------
    actual : Mapping[str, object]
        Mapping returned by the handler capture fixture.
    expected : Mapping[str, object]
        Expected key/value pairs that must be present in *actual*.
    """
    for key, value in expected.items():
        assert actual[key] == value


@dataclass(frozen=True, slots=True)
class DummyCfg:
    """Minimal stand-in pipeline config for CLI helper tests."""

    name: str = 'p1'
    version: str = 'v1'
    sources: list[object] = field(
        default_factory=lambda: [types.SimpleNamespace(name='s1')],
    )
    targets: list[object] = field(
        default_factory=lambda: [types.SimpleNamespace(name='t1')],
    )
    transforms: list[object] = field(
        default_factory=lambda: [types.SimpleNamespace(name='tr1')],
    )
    jobs: list[object] = field(
        default_factory=lambda: [types.SimpleNamespace(name='j1')],
    )


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='capture_handler')
def capture_handler_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> CaptureHandler:
    """
    Patch a handler function and capture the kwargs it receives.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.

    Returns
    -------
    CaptureHandler
        Callable that records handler keyword arguments.
    """

    def _capture(module: object, attr: str) -> dict[str, object]:
        calls: dict[str, object] = {}

        def _stub(**kwargs: object) -> int:
            calls.update(kwargs)
            return 0

        monkeypatch.setattr(module, attr, _stub)
        return calls

    return _capture


@pytest.fixture(name='capture_io')
def capture_io_fixture(monkeypatch: pytest.MonkeyPatch) -> CaptureIo:
    """
    Patch handler functions and capture CLI output.
    Returns a dict with lists of call args for each function.

    Parameters
    ----------
    monkeypatch : pytest.MonkeyPatch
        Pytest monkeypatch fixture.

    Returns
    -------
    CaptureIo
        Mapping of captured IO call arguments by function name.
    """
    import etlplus.cli.io as _io

    calls: CaptureIo = {
        'emit_or_write': [],
        'emit_json': [],
        'print_json': [],
    }
    monkeypatch.setattr(
        _io,
        'emit_or_write',
        lambda *a, **k: calls['emit_or_write'].append((a, k)),
    )
    monkeypatch.setattr(
        _io,
        'emit_json',
        lambda *a, **k: calls['emit_json'].append((a, k)),
    )
    monkeypatch.setattr(
        _io,
        'print_json',
        lambda *a, **k: calls['print_json'].append((a, k)),
    )
    return calls


@pytest.fixture(name='csv_text')
def csv_text_fixture() -> str:
    """Return sample CSV text."""
    return CSV_TEXT


@pytest.fixture(name='dummy_cfg')
def dummy_cfg_fixture() -> PipelineConfig:
    """Return a minimal dummy pipeline config."""
    return cast(PipelineConfig, DummyCfg())


@pytest.fixture(name='invoke_cli')
def invoke_cli_fixture(runner: CliRunner) -> Callable[..., Result]:
    """Invoke the Typer CLI with convenience defaults."""

    def _invoke(*args: str) -> Result:
        return runner.invoke(cli_app, list(args))

    return _invoke


@pytest.fixture(name='runner')
def runner_fixture() -> CliRunner:
    """Return a reusable Typer CLI runner."""
    return CliRunner()


@pytest.fixture(name='stub_command')
def stub_command_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[Callable[..., object]], None]:
    """Install a Typer command stub that delegates to the provided action."""

    def _install(action: Callable[..., object]) -> None:
        class _StubCommand:
            def main(
                self,
                *,
                args: list[str],
                prog_name: str,
                standalone_mode: bool,
            ) -> object:
                return action(
                    args=args,
                    prog_name=prog_name,
                    standalone_mode=standalone_mode,
                )

        monkeypatch.setattr(
            typer.main,
            'get_command',
            lambda _app: _StubCommand(),
        )

    return _install


@pytest.fixture(name='widget_spec_paths')
def widget_spec_paths_fixture(tmp_path: Path) -> tuple[Path, Path]:
    """Return paths for a widget spec and output SQL file."""
    spec = {
        'schema': 'dbo',
        'table': 'Widget',
        'columns': [
            {'name': 'Id', 'type': 'int', 'nullable': False},
            {'name': 'Name', 'type': 'nvarchar(50)', 'nullable': True},
        ],
        'primary_key': {'columns': ['Id']},
    }
    spec_path = tmp_path / 'spec.json'
    out_path = tmp_path / 'out.sql'
    spec_path.write_text(json.dumps(spec))
    return spec_path, out_path
