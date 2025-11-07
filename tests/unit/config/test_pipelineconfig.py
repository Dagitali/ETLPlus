"""
ETLPlus Config Tests
====================

Unit tests for the ETLPlus configuration models.

Notes
-----
These tests cover the loading and parsing of pipeline configuration
YAML files, including variable substitution and profile handling.
"""
from __future__ import annotations

from etlplus.config import PipelineConfig


def test_from_yaml_includes_profile_env_in_substitution(tmp_path):
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

    p = tmp_path / 'cfg.yml'
    p.write_text(yml, encoding='utf-8')

    cfg = PipelineConfig.from_yaml(p, substitute=True, env={})
    # After substitution, re-parse should keep the resolved path
    s = next(s for s in cfg.sources if s.name == 's')
    assert getattr(s, 'path', None) == 'bar-123.json'
