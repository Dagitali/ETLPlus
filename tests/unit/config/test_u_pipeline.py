"""
:mod:`tests.unit.config.test_u_pipeline` module.

Unit tests for ``etlplus.config.pipeline``.

Notes
-----
- Exercises multiple sources/targets and unresolved variables.
- Uses internal ``_build_connectors`` helper to exercise parsing logic.
- Validates profile environment is included in substitution.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from etlplus.config.connector import ConnectorApi
from etlplus.config.connector import ConnectorDb
from etlplus.config.connector import ConnectorFile
from etlplus.config.pipeline import PipelineConfig
from etlplus.config.pipeline import _build_connectors

MULTI_SOURCE_YAML = """
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

CONNECTOR_CASES = (
    pytest.param(
        'sources',
        [
            {'name': 'csv_in', 'type': 'file', 'path': '/tmp/in.csv'},
            {
                'name': 'service_in',
                'type': 'api',
                'api': 'github',
                'endpoint': 'issues',
            },
            {'name': 'analytics', 'type': 'database', 'table': 'events'},
            123,
            {'name': 'weird', 'type': 'unknown'},
            {'type': 'file'},
        ],
        {ConnectorFile, ConnectorApi, ConnectorDb},
        id='sources',
    ),
    pytest.param(
        'targets',
        [
            {'name': 'csv_out', 'type': 'file', 'path': '/tmp/out.csv'},
            {'name': 'sink', 'type': 'database', 'table': 'events_out'},
            {
                'name': 'svc',
                'type': 'api',
                'api': 'hub',
                'endpoint': 'post',
            },
            {'name': 'bad', 'type': 'unknown'},
        ],
        {ConnectorFile, ConnectorDb, ConnectorApi},
        id='targets',
    ),
)


@pytest.fixture(name='pipeline_multi_cfg')
def pipeline_multi_cfg_fixture(
    tmp_path: Path,
    pipeline_yaml_factory: Callable[[str, Path], Path],
    pipeline_from_yaml_factory: Callable[..., PipelineConfig],
) -> PipelineConfig:
    """Build a :class:`PipelineConfig` with multiple sources/targets.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory managed by pytest.
    pipeline_yaml_factory : Callable[[str, Path], Path]
        Factory that writes YAML content to disk.
    pipeline_from_yaml_factory : Callable[..., PipelineConfig]
        Factory that instantiates :class:`PipelineConfig` objects.

    Returns
    -------
    PipelineConfig
        Parsed configuration with substitution enabled.
    """

    path = pipeline_yaml_factory(MULTI_SOURCE_YAML.strip(), tmp_path)
    return pipeline_from_yaml_factory(path, substitute=True, env={})


# SECTION: TESTS ============================================================ #


@pytest.mark.unit
class TestPipelineBuildConnectors:
    """
    Unit test suite for :func:`_build_connectors`.

    Notes
    -----
    Tests connector parsing for sources and targets, including skipping
    malformed and unsupported entries.
    """

    @pytest.mark.parametrize(
        ('key', 'entries', 'expected_types'),
        CONNECTOR_CASES,
    )
    def test_build_connectors_filters_invalid_entries(
        self,
        key: str,
        entries: list[Any],
        expected_types: set[type],
    ) -> None:
        """Ensure :func:`_build_connectors` filters malformed entries."""
        payload = {key: entries}
        items = _build_connectors(payload, key)

        assert len(items) == len(expected_types)
        assert {type(item) for item in items} == expected_types


class TestPipelineConfig:
    """
    Unit test suite for :class:`PipelineConfig`.
    """

    def test_from_yaml_includes_profile_env_in_substitution(
        self,
        tmp_path: Path,
        pipeline_yaml_factory: Callable[[str, Path], Path],
        pipeline_from_yaml_factory: Callable[..., PipelineConfig],
    ) -> None:  # noqa: D401
        """
        Test that :class:`PipelineConfig` includes profile environment
        variables in substitution when loaded from YAML.

        Parameters
        ----------
        tmp_path : Path
            Temporary directory path.
        pipeline_yaml_factory : Callable[[str, Path], Path]
            Factory to create a pipeline YAML file.
        pipeline_from_yaml_factory : Callable[..., PipelineConfig]
            Factory to create a PipelineConfig from YAML.
        """
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

    @pytest.mark.parametrize(
        ('collection', 'name', 'expected_path'),
        [
            pytest.param(
                'sources',
                's1',
                'one-${B}.json',
                id='source-missing',
            ),
            pytest.param('sources', 's2', 'literal.json', id='source-literal'),
            pytest.param('targets', 't1', 'out-one.json', id='target'),
        ],
    )
    def test_multiple_sources_targets_and_missing_vars(
        self,
        collection: str,
        name: str,
        expected_path: str,
        pipeline_multi_cfg: PipelineConfig,
    ) -> None:
        """
        Test that :class:`PipelineConfig` correctly handles multiple sources,
        targets, and missing variables during substitution.

        Parameters
        ----------
        collection : str
            Either ``'sources'`` or ``'targets'``.
        name : str
            Connector name to inspect.
        expected_path : str
            Expected path after substitution.
        pipeline_multi_cfg : PipelineConfig
            Fixture containing the parsed configuration.
        """
        container = getattr(pipeline_multi_cfg, collection)
        connector = next(item for item in container if item.name == name)
        assert getattr(connector, 'path', None) == expected_path
