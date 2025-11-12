"""
``tests.integration.test_pipeline_yaml_load`` module.

Pipeline YAML load integration test suite. Parametrized checks to ensure the
repository pipeline YAML parses correctly with and without environment-variable
substitution enabled.

Notes
-----
- Uses ``PipelineConfig.from_yaml`` on the repo's example config.
- Asserts basic API modeling and presence of expected endpoints.
"""
from __future__ import annotations

import pytest

from etlplus.config import PipelineConfig


# SECTION: TESTS ============================================================ #


class TestPipelineYamlLoad:
    @pytest.mark.parametrize('substitute', [False, True])
    def test_load_repo_pipeline_yaml(
        self,
        substitute: bool,
    ) -> None:  # noqa: D401
        # Ensure the repository pipeline YAML parses under current models.
        cfg = PipelineConfig.from_yaml(
            'in/pipeline.yml', substitute=substitute,
        )
        assert isinstance(cfg, PipelineConfig)

        # Basic sanity checks on REST API modeling.
        assert 'github' in cfg.apis
        gh = cfg.apis['github']
        assert 'org_repos' in gh.endpoints

        # Profiles modeled if present.
        assert isinstance(getattr(gh, 'profiles', {}), dict)
