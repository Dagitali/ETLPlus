"""
:mod:`tests.integration.cli.test_i_cli_render` module.

Integration-scope smoke tests for the ``etlplus render`` CLI command.
"""

from __future__ import annotations

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

# SECTION: TYPE ALIASES ===================================================== #


type RenderInputCase = tuple[str, Path, str]


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
    return path_flag, getattr(render_input, path_attr), render_input.table_name

# SECTION: TESTS ============================================================ #


def test_input_emits_sql(
    cli_invoke: CliInvoke,
    render_input_case: RenderInputCase,
) -> None:
    """Test rendering SQL from supported render input shapes."""
    path_flag, input_path, table_name = render_input_case
    code, out, err = cli_invoke(
        (
            'render',
            path_flag,
            str(input_path),
            '--template',
            'ddl',
        ),
    )
    assert_cli_success(code, err)
    assert 'CREATE TABLE' in out
    assert table_name in out
