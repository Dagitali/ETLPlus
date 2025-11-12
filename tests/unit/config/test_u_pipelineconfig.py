"""
``tests.unit.config.test_u_pipelineconfig`` module.


Unit tests for the ETLPlus pipeline configuration models.

Notes
-----
Covers YAML loading, variable substitution (present and missing vars), and
multi-source/target handling.
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
        s_item = next(s for s in cfg.sources if s.name == 's')
        assert getattr(s_item, 'path', None) == 'bar-123.json'

    def test_multiple_sources_targets_and_missing_vars(
        self,
        tmp_path,
        pipeline_yaml_factory,
        pipeline_from_yaml_factory,
    ) -> None:
        yml = (
            """
name: TestMulti
vars:
  A: one
sources:
  - name: s1
    type: file
    format: json
    path: "${A}-${B}.json"
  - name: s2
    type: file
    format: json
    path: "literal.json"
targets:
  - name: t1
    type: file
    format: json
    path: "out-${A}.json"
jobs: []
"""
        ).strip()

        p = pipeline_yaml_factory(yml, tmp_path)
        cfg = pipeline_from_yaml_factory(p, substitute=True, env={})
        s1 = next(s for s in cfg.sources if s.name == 's1')
        s2 = next(s for s in cfg.sources if s.name == 's2')
        t1 = next(t for t in cfg.targets if t.name == 't1')

        # Variable A substituted; missing B should remain unresolved.
        assert getattr(s1, 'path', None) == 'one-${B}.json'
        assert getattr(s2, 'path', None) == 'literal.json'
        assert getattr(t1, 'path', None) == 'out-one.json'
