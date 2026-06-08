"""
:mod:`tests.integration.cli.test_i_cli_render` module.

Integration-scope smoke tests for the ``etlplus render`` CLI command.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests.integration.cli.pytest_cli_integration_support import assert_cli_success

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.pytest_shared_support import CliInvoke

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]

# SECTION: DATA CLASSES ===================================================== #


@dataclass(frozen=True, slots=True)
class RenderInputCase:
    """Prepared render command input."""

    path_flag: str
    input_path: Path
    table_name: str


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(
    name='render_input_case',
    params=[
        pytest.param(
            ('pipeline_table_schemas_config', '--config', 'config_path'),
            id='config',
        ),
        pytest.param(('table_spec', '--spec', 'spec_path'), id='spec'),
    ],
)
def render_input_case_fixture(
    request: pytest.FixtureRequest,
) -> RenderInputCase:
    """Return one prepared render command input case."""
    fixture_name, path_flag, path_attr = request.param
    render_input = request.getfixturevalue(fixture_name)
    return RenderInputCase(
        path_flag=path_flag,
        input_path=getattr(render_input, path_attr),
        table_name=render_input.table_name,
    )

# SECTION: TESTS ============================================================ #


class TestCliRender:
    """Smoke tests for the ``etlplus render`` CLI command."""

    def test_input_emits_sql(
        self,
        cli_invoke: CliInvoke,
        render_input_case: RenderInputCase,
    ) -> None:
        """Test rendering SQL from supported render input shapes."""
        code, out, err = cli_invoke(
            (
                'render',
                render_input_case.path_flag,
                str(render_input_case.input_path),
                '--template',
                'ddl',
            ),
        )
        assert_cli_success(code, err)
        assert 'CREATE TABLE' in out
        assert render_input_case.table_name in out
