"""
:mod:`tests.integration.test_i_pipeline_yaml_load` module.

Pipeline YAML load integration test suite. Parametrized checks to ensure the
repository pipeline YAML parses correctly with and without environment-
variable substitution enabled.

Notes
-----
- Uses ``PipelineConfig.from_yaml`` on the repo's example config.
- Asserts basic API modeling and presence of expected endpoints.
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from etlplus.workflow import PipelineConfig

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.integration


# SECTION: TESTS ============================================================ #


class TestPipelineYamlLoad:
    """Integration test suite for pipeline YAML loading."""

    @pytest.mark.parametrize('substitute', [False, True])
    def test_load_repo_pipeline_yaml(
        self,
        substitute: bool,
    ) -> None:
        """
        Test loading the repository pipeline YAML with optional environment
        variable substitution.
        """

        # Ensure the repository pipeline YAML parses under current models.
        cfg = PipelineConfig.from_yaml(
            'examples/configs/pipeline.yml',
            substitute=substitute,
        )
        assert isinstance(cfg, PipelineConfig)

        # Basic sanity checks on REST API modeling.
        assert 'github' in cfg.apis
        gh = cfg.apis['github']
        assert 'org_repos' in gh.endpoints

        # Profiles modeled if present (mapping proxies acceptable).
        assert isinstance(getattr(gh, 'profiles', {}), Mapping)
