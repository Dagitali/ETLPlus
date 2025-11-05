"""
Pipeline YAML load test
=======================

Tests loading the repository pipeline YAML to ensure it parses correctly
under current configuration models.
"""
from __future__ import annotations

from etlplus.config import PipelineConfig


def test_load_repo_pipeline_yaml_no_substitution():
    # Ensure the repository pipeline YAML parses under current models
    cfg = PipelineConfig.from_yaml('in/pipeline.yml', substitute=False)
    assert isinstance(cfg, PipelineConfig)
    # Basic sanity checks on REST API modeling
    assert 'github' in cfg.apis
    gh = cfg.apis['github']
    assert 'org_repos' in gh.endpoints
    # Profiles modeled if present
    assert isinstance(getattr(gh, 'profiles', {}), dict)
