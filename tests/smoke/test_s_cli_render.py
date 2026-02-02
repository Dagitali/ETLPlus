"""
:mod:`tests.smoke.test_s_cli_render` module.

Smoke test suite for the ``etlplus render`` CLI command.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.smoke.conftest import PipelineSchema
    from tests.smoke.conftest import TableSpec


# SECTION: MARKERS ========================================================== #


# Directory-level marker for smoke tests.
pytestmark = pytest.mark.smoke


# SECTION: TESTS ============================================================ #


class TestCliRender:
    """Smoke test suite for the ``etlplus render`` CLI command."""

    def test_config_emits_sql(
        self,
        cli_invoke: CliInvoke,
        pipeline_table_schemas_config: PipelineSchema,
    ) -> None:
        """Render SQL from a pipeline config containing table_schemas."""
        cfg = pipeline_table_schemas_config
        code, out, err = cli_invoke(
            ('render', '--config', str(cfg.config_path), '--template', 'ddl'),
        )
        assert code == 0
        assert err.strip() == ''
        assert 'CREATE TABLE' in out
        assert cfg.table_name in out

    def test_spec_emits_sql(
        self,
        cli_invoke: CliInvoke,
        table_spec: TableSpec,
    ) -> None:
        """Test rendering a minimal table spec and ensure SQL is produced."""
        code, out, err = cli_invoke(
            (
                'render',
                '--spec',
                str(table_spec.spec_path),
                '--template',
                'ddl',
            ),
        )
        assert code == 0
        assert err.strip() == ''
        assert 'CREATE TABLE' in out
        assert table_spec.table_name in out
