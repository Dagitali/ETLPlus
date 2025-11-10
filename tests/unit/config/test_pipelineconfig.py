"""
tests.unit.config.test_pipelineconfig unit tests module.


Unit tests for the ETLPlus configuration models.

Notes
-----
These tests cover the loading and parsing of pipeline configuration
YAML files, including variable substitution and profile handling.
"""
from __future__ import annotations


# SECTION: TESTS ============================================================ #


class TestPipelineConfig:
    def test_from_yaml_includes_profile_env_in_substitution(
        self,
        tmp_path,
        pipeline_yaml_factory,
        pipeline_from_yaml_factory,
    ) -> None:  # noqa: D401
        yml = (
            """
name: Test
profile:
  env:
    FOO: bar
vars:
  X: 123
sources:
  - name: s
    type: file
    format: json
    path: "${FOO}-${X}.json"
targets: []
jobs: []
"""
        ).strip()

        p = pipeline_yaml_factory(yml, tmp_path)
        cfg = pipeline_from_yaml_factory(p, substitute=True, env={})

        # After substitution, re-parse should keep the resolved path.
        s = next(s for s in cfg.sources if s.name == 's')
        assert getattr(s, 'path', None) == 'bar-123.json'
