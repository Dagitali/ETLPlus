"""
:mod:`tests.integration.cli.test_i_cli_render` module.

Integration-scope smoke tests for the ``etlplus render`` CLI command.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.integration.cli.pytest_cli_integration_support import assert_cli_success

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.pytest_shared_support import CliInvoke

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]

# SECTION: TESTS ============================================================ #


class TestCliRender:
    """Smoke tests for the ``etlplus render`` CLI command."""

    @pytest.mark.parametrize(
        ('fixture_name', 'path_flag'),
        [
            pytest.param(
                'pipeline_table_schemas_config',
                '--config',
                id='config',
            ),
            pytest.param('table_spec', '--spec', id='spec'),
        ],
    )
    def test_input_emits_sql(
        self,
        cli_invoke: CliInvoke,
        request: pytest.FixtureRequest,
        fixture_name: str,
        path_flag: str,
    ) -> None:
        """Test rendering SQL from supported render input shapes."""
        render_input = request.getfixturevalue(
            fixture_name,
        )
        input_path = (
            render_input.config_path
            if hasattr(render_input, 'config_path')
            else render_input.spec_path
        )
        code, out, err = cli_invoke(
            ('render', path_flag, str(input_path), '--template', 'ddl'),
        )
        assert_cli_success(code, err)
        assert 'CREATE TABLE' in out
        assert render_input.table_name in out
