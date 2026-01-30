"""
:mod:`tests.unit.workflow.test_u_workflow_pipeline` module.

Unit tests for :mod:`etlplus.workflow.pipeline`.

Notes
-----
- Exercises multiple sources/targets and unresolved variables.
- Uses internal ``_build_connectors`` helper to exercise parsing logic.
- Validates profile environment is included in substitution.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from etlplus.connector import ConnectorApi
from etlplus.connector import ConnectorDb
from etlplus.connector import ConnectorFile
from etlplus.workflow.pipeline import PipelineConfig
from etlplus.workflow.pipeline import _collect_parsed
from etlplus.workflow.pipeline import _parse_connector_entry

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


@dataclass(frozen=True, slots=True)
class ConnectorCase:
    """Connector collection test case definition."""

    collection: str
    entries: list[Any]
    expected_types: set[type]


CONNECTOR_CASES: tuple[ConnectorCase, ...] = (
    ConnectorCase(
        collection='sources',
        entries=[
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
        expected_types={ConnectorFile, ConnectorApi, ConnectorDb},
    ),
    ConnectorCase(
        collection='targets',
        entries=[
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
        expected_types={ConnectorFile, ConnectorDb, ConnectorApi},
    ),
)

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


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='connector_path_lookup')
def connector_path_lookup_fixture(
    pipeline_multi_cfg: PipelineConfig,
) -> Callable[[str, str], str | None]:
    """Provide a helper to fetch connector paths by collection/name."""

    def _lookup(collection: str, name: str) -> str | None:
        container = getattr(pipeline_multi_cfg, collection)
        connector = next(item for item in container if item.name == name)
        return getattr(connector, 'path', None)

    return _lookup


@pytest.fixture(name='pipeline_builder')
def pipeline_builder_fixture(
    tmp_path: Path,
    pipeline_yaml_factory: Callable[[str, Path], Path],
    pipeline_from_yaml_factory: Callable[..., PipelineConfig],
) -> Callable[..., PipelineConfig]:
    """
    Build :class:`PipelineConfig` instances from inline YAML strings.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory managed by pytest.
    pipeline_yaml_factory : Callable[[str, Path], Path]
        Helper that writes YAML text to disk.
    pipeline_from_yaml_factory : Callable[..., PipelineConfig]
        Factory that parses YAML into a :class:`PipelineConfig`.

    Returns
    -------
    Callable[..., PipelineConfig]
        Function that renders YAML text to a config with optional overrides.
    """

    def _build(
        yaml_text: str,
        *,
        substitute: bool = True,
        env: dict[str, str] | None = None,
    ) -> PipelineConfig:
        path = pipeline_yaml_factory(yaml_text.strip(), tmp_path)
        return pipeline_from_yaml_factory(
            path,
            substitute=substitute,
            env=env or {},
        )

    return _build


@pytest.fixture(name='pipeline_multi_cfg')
def pipeline_multi_cfg_fixture(
    pipeline_builder: Callable[..., PipelineConfig],
) -> PipelineConfig:
    """Build a :class:`PipelineConfig` with multiple sources/targets.

    Parameters
    ----------
    pipeline_builder : Callable[..., PipelineConfig]
        Fixture that converts inline YAML strings into pipeline configs.

    Returns
    -------
    PipelineConfig
        Parsed configuration with substitution enabled.
    """
    return pipeline_builder(MULTI_SOURCE_YAML)


# SECTION: TESTS ============================================================ #


class TestCollectParsed:
    """
    Unit test suite for :func:`_collect_parsed`.

    Notes
    -----
    Tests connector parsing for sources and targets, including skipping
    malformed and unsupported entries.
    """

    @pytest.mark.parametrize(
        'case',
        CONNECTOR_CASES,
        ids=lambda c: c.collection,
    )
    def test_collect_parsed_filters_invalid_entries(
        self,
        case: ConnectorCase,
    ) -> None:
        """Test that :func:`_collect_parsed` filters malformed entries."""
        payload = {case.collection: case.entries}
        items = _collect_parsed(
            payload.get(case.collection, []),
            _parse_connector_entry,
        )

        assert len(items) == len(case.expected_types)
        assert {type(item) for item in items} == case.expected_types


class TestPipelineConfig:
    """
    Unit test suite for :class:`PipelineConfig`.
    """

    def test_from_yaml_includes_profile_env_in_substitution(
        self,
        pipeline_builder: Callable[..., PipelineConfig],
    ) -> None:  # noqa: D401
        """
        Test that :class:`PipelineConfig` includes profile environment
        variables in substitution when loaded from YAML.

        Parameters
        ----------
        pipeline_builder : Callable[..., PipelineConfig]
            Fixture that renders YAML text into parsed configs.
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

        cfg = pipeline_builder(yml)

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
        connector_path_lookup: Callable[[str, str], str | None],
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
        connector_path_lookup : Callable[[str, str], str | None]
            Helper that fetches connector paths from the parsed config.
        """
        path = connector_path_lookup(collection, name)
        assert path == expected_path

    def test_table_schemas_are_parsed(
        self,
        pipeline_builder: Callable[..., PipelineConfig],
    ) -> None:
        """
        Test that table_schemas entries are preserved when loading YAML.
        """
        yml = (
            """
name: TablesOnly
table_schemas:
  - schema: dbo
    table: Customers
    columns:
      - name: CustomerId
        type: int
        nullable: false
sources: []
targets: []
jobs: []
            """
        ).strip()

        cfg = pipeline_builder(yml)

        assert len(cfg.table_schemas) == 1
        spec = cfg.table_schemas[0]
        assert spec['table'] == 'Customers'
        assert spec['columns'][0]['name'] == 'CustomerId'
