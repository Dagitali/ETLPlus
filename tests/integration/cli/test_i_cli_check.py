"""
:mod:`tests.integration.cli.test_i_cli_check` module.

Integration-scope smoke tests for the ``etlplus check`` CLI command.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser
    from tests.integration.cli.conftest import PipelineConfigFactory

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]


# SECTION: TESTS ============================================================ #


class TestCliCheck:
    """Smoke tests for the ``etlplus check`` CLI command."""

    def test_jobs_lists_job(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        pipeline_config_factory: PipelineConfigFactory,
        sample_records: list[dict[str, Any]],
    ) -> None:
        """Test that ``check --jobs`` returns the configured job name."""
        cfg = pipeline_config_factory(sample_records)
        code, out, err = cli_invoke(
            ('check', '--config', str(cfg.config_path), '--jobs'),
        )
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert cfg.job_name in payload.get('jobs', [])
