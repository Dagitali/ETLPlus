"""
:mod:`tests.integration.test_i_pipeline_yaml_load` module.

Pipeline YAML load integration test suite. Parametrized checks to ensure the
repository pipeline YAML parses correctly with and without environment-
variable substitution enabled.

Notes
-----
- Uses ``Config.from_yaml`` on the repo's example config.
- Asserts basic API modeling and presence of expected endpoints.
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from etlplus import Config

# SECTION: TESTS ============================================================ #


class TestConfigLoad:
    """Integration test suite for configuration loading."""

    @pytest.mark.parametrize('substitute', [False, True])
    def test_load_repo_pipeline_yaml(
        self,
        substitute: bool,
    ) -> None:
        """
        Test loading the configuration file with optional environment variable
        substitution.
        """

        # Ensure the configuration file parses under current models.
        cfg = Config.from_yaml(
            'examples/configs/pipeline.yml',
            substitute=substitute,
        )
        assert isinstance(cfg, Config)

        # Basic sanity checks on REST API modeling.
        assert 'github' in cfg.apis
        gh = cfg.apis['github']
        assert 'org_repos' in gh.endpoints

        # Profiles modeled if present (mapping proxies acceptable).
        assert isinstance(getattr(gh, 'profiles', {}), Mapping)
