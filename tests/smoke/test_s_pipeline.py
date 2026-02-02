"""
:mod:`tests.smoke.test_s_pipeline` module.

Smoke test suite exercising a minimal file→file job via the CLI. Parametrized
to verify both empty and non-empty inputs.

Notes
-----
- Builds a transient pipeline YAML string per test run.
- Invokes ``etlplus run --job <job>`` end-to-end.
- Validates output file contents against input data shape.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.smoke.conftest import PipelineConfigFactory

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.smoke


# SECTION: TESTS ============================================================ #


class TestPipeline:
    """Smoke test suite for file→file job via CLI."""

    def test_file_to_file(
        self,
        cli_invoke: CliInvoke,
        pipeline_config_factory: PipelineConfigFactory,
        sample_records: list[dict[str, object]],
    ) -> None:
        """Test file→file jobs via CLI for multiple input datasets."""
        cfg = pipeline_config_factory(sample_records)

        code, out, err = cli_invoke(
            (
                'run',
                '--config',
                str(cfg.config_path),
                '--job',
                cfg.job_name,
            ),
        )
        assert err == ''
        assert code == 0

        payload = json.loads(out)

        # CLI should have printed a JSON object with status ok.
        assert payload.get('status') == 'ok'
        assert isinstance(payload.get('result'), dict)
        assert payload['result'].get('status') == 'success'

        # Output file should exist and match input data.
        assert cfg.output_path.exists()
        with cfg.output_path.open('r', encoding='utf-8') as f:
            out_data = json.load(f)
        assert out_data == sample_records
