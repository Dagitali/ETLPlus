"""
``tests.unit.config.test_u_pipeline`` module.

Unit tests for ``etlplus.config.pipeline``.

Notes
-----
- Exercises multiple sources/targets and unresolved variables.
- Uses internal ``_build_connectors`` helper to exercise parsing logic.
- Validates profile environment is included in substitution.
"""
from __future__ import annotations

from etlplus.config.connector import ConnectorApi
from etlplus.config.connector import ConnectorDb
from etlplus.config.connector import ConnectorFile
from etlplus.config.pipeline import _build_connectors


# SECTION: TESTS ============================================================ #


class TestPipelineBuildConnectors:
    """
    Unit test suite for the :func:`_build_connectors` function.
    """

    def test_build_connectors_skips_malformed_and_unsupported(
        self,
    ) -> None:  # noqa: D401
        raw = {
            'sources': [
                {'name': 'csv_in', 'type': 'file', 'path': '/tmp/in.csv'},
                {
                    'name': 'service_in',
                    'type': 'api',
                    'api': 'github',
                    'endpoint': 'issues',
                },
                {'name': 'analytics', 'type': 'database', 'table': 'events'},
                123,  # Skip non-dict.
                {'name': 'weird', 'type': 'unknown'},  # Skip Unsupported.
                {'type': 'file'},  # Skip missing name.
            ],
        }

        items = _build_connectors(raw, 'sources')

        # Expect only the three valid connectors constructed.
        assert len(items) == 3
        assert any(isinstance(c, ConnectorFile) for c in items)
        assert any(isinstance(c, ConnectorApi) for c in items)
        assert any(isinstance(c, ConnectorDb) for c in items)

    def test_build_connectors_for_targets_key(self):  # noqa: D401
        raw = {
            'targets': [
                {'name': 'csv_out', 'type': 'file', 'path': '/tmp/out.csv'},
                {'name': 'sink', 'type': 'database', 'table': 'events_out'},
                {
                    'name': 'svc',
                    'type': 'api',
                    'api': 'hub',
                    'endpoint': 'post',
                },
                {'name': 'bad', 'type': 'unknown'},   # Skipped.
            ],
        }

        items = _build_connectors(raw, 'targets')
        assert len(items) == 3
        assert any(isinstance(c, ConnectorFile) for c in items)
        assert any(isinstance(c, ConnectorDb) for c in items)
        assert any(isinstance(c, ConnectorApi) for c in items)


class TestPipelineConfig:
    """
    Unit test suite for the :class:`PipelineConfig` class.
    """

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
