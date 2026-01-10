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
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Final
from typing import cast

import pytest
from typer.testing import CliRunner
from typer.testing import Result

from etlplus.cli.commands import app as cli_app
from etlplus.config import PipelineConfig

# SECTION: HELPERS ======================================================== #


CSV_TEXT: Final[str] = 'a,b\n1,2\n3,4\n'


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
