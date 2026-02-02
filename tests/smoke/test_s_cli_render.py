"""
:mod:`tests.smoke.test_s_cli_render` module.

Smoke test suite for the ``etlplus render`` CLI command.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.smoke.conftest import TableSpecFixture


pytestmark = pytest.mark.smoke


class TestCliRender:
    """Smoke test suite for the ``etlplus render`` CLI command."""

    def test_spec_emits_sql(
        self,
        cli_invoke: CliInvoke,
        table_spec: TableSpecFixture,
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
