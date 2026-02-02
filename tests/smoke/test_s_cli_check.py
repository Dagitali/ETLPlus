"""
:mod:`tests.smoke.test_s_cli_check` module.

Smoke test suite for the ``etlplus check`` CLI command.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser
    from tests.smoke.conftest import PipelineConfigFactory


pytestmark = pytest.mark.smoke


class TestCliCheck:
    """Smoke test suite for the ``etlplus check`` CLI command."""

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
