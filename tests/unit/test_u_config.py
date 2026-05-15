"""
:mod:`tests.unit.test_u_config` module.

Unit tests for :mod:`etlplus._config`.

Notes
-----
- Exercises multiple sources/targets and unresolved variables.
- Uses internal ``_build_connectors`` helper to exercise parsing logic.
- Validates profile environment is included in substitution.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

import etlplus._config as config_mod
from etlplus import Config
from etlplus._config import _collect_parsed
from etlplus._config import _parse_connector_entry
from etlplus.connector import ConnectorApi
from etlplus.connector import ConnectorDb
from etlplus.connector import ConnectorFile

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


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
    pipeline_multi_cfg: Config,
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
    pipeline_from_yaml_factory: Callable[..., Config],
) -> Callable[..., Config]:
    """
    Build :class:`Config` instances from inline YAML strings.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory managed by pytest.
    pipeline_yaml_factory : Callable[[str, Path], Path]
        Helper that writes YAML text to disk.
    pipeline_from_yaml_factory : Callable[..., Config]
        Factory that parses YAML into a :class:`Config`.

    Returns
    -------
    Callable[..., Config]
        Function that renders YAML text to a config with optional overrides.
    """

    def _build(
        yaml_text: str,
        *,
        substitute: bool = True,
        env: dict[str, str] | None = None,
    ) -> Config:
        path = pipeline_yaml_factory(yaml_text.strip(), tmp_path)
        return pipeline_from_yaml_factory(
            path,
            substitute=substitute,
            env=env or {},
        )

    return _build


@pytest.fixture(name='pipeline_multi_cfg')
def pipeline_multi_cfg_fixture(
    pipeline_builder: Callable[..., Config],
) -> Config:
    """Build a :class:`Config` with multiple sources/targets.

    Parameters
    ----------
    pipeline_builder : Callable[..., Config]
        Fixture that converts inline YAML strings into pipeline configs.

    Returns
    -------
    Config
        Parsed configuration with substitution enabled.
    """
    return pipeline_builder(MULTI_SOURCE_YAML)


# SECTION: TESTS ============================================================ #


class TestCollectParsed:
    """
    Unit tests for :func:`_collect_parsed`.

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


class TestConfig:
    """
    Unit tests for :class:`Config`.
    """

    def test_from_dict_parses_apis_and_filters_non_mapping_table_specs(
        self,
    ) -> None:
        """
        Test that parsing non-empty APIs and tolerant table_schemas filtering.
        """
        raw = {
            'name': 'Test',
            'apis': {
                'svc': {
                    'base_url': 'https://example.test',
                    'endpoints': {'users': '/users'},
                },
            },
            'table_schemas': [
                {
                    'schema': 'dbo',
                    'table': 'customers',
                },
                'skip-me',
            ],
            'sources': [],
            'targets': [],
            'jobs': [],
        }

        cfg = Config.from_dict(raw)

        assert 'svc' in cfg.apis
        assert len(cfg.table_schemas) == 1
        assert cfg.table_schemas[0]['table'] == 'customers'

    def test_from_dict_parses_history_defaults(
        self,
    ) -> None:
        """Test that :class:`Config` parses one optional history block."""
        cfg = Config.from_dict(
            {
                'name': 'History Config Test',
                'history': {
                    'enabled': False,
                    'backend': 'jsonl',
                    'state_dir': './.etlplus-state',
                    'capture_tracebacks': True,
                },
                'sources': [],
                'targets': [],
                'jobs': [],
            },
        )

        assert cfg.history.enabled is False
        assert cfg.history.backend == 'jsonl'
        assert cfg.history.state_dir == './.etlplus-state'
        assert cfg.history.capture_tracebacks is True

    def test_from_dict_parses_schedules(
        self,
    ) -> None:
        """Test that :class:`Config` parses one optional schedules block."""
        cfg = Config.from_dict(
            {
                'name': 'Schedule Config Test',
                'sources': [],
                'targets': [],
                'jobs': [],
                'schedules': [
                    {
                        'name': 'nightly_all',
                        'cron': '0 2 * * *',
                        'timezone': 'UTC',
                        'paused': False,
                        'target': {
                            'run_all': True,
                        },
                        'backfill': {
                            'enabled': True,
                            'max_catchup_runs': 3,
                            'start_at': '2026-05-01T00:00:00Z',
                        },
                    },
                    {
                        'name': 'customers_every_30m',
                        'interval': {
                            'minutes': 30,
                        },
                        'target': {
                            'job': 'job-a',
                        },
                    },
                ],
            },
        )

        assert len(cfg.schedules) == 2
        assert cfg.schedules[0].name == 'nightly_all'
        assert cfg.schedules[0].cron == '0 2 * * *'
        assert cfg.schedules[0].target is not None
        assert cfg.schedules[0].target.run_all is True
        assert cfg.schedules[0].backfill is not None
        assert cfg.schedules[0].backfill.max_catchup_runs == 3
        assert cfg.schedules[1].interval is not None
        assert cfg.schedules[1].interval.minutes == 30
        assert cfg.schedules[1].target is not None
        assert cfg.schedules[1].target.job == 'job-a'

    def test_from_dict_parses_telemetry_defaults(
        self,
    ) -> None:
        """Test that :class:`Config` parses one optional telemetry block."""
        cfg = Config.from_dict(
            {
                'name': 'Telemetry Config Test',
                'telemetry': {
                    'enabled': True,
                    'exporter': 'opentelemetry',
                    'service_name': 'etlplus-tests',
                },
                'sources': [],
                'targets': [],
                'jobs': [],
            },
        )

        assert cfg.telemetry.enabled is True
        assert cfg.telemetry.exporter == 'opentelemetry'
        assert cfg.telemetry.service_name == 'etlplus-tests'

    def test_from_yaml_includes_profile_env_in_substitution(
        self,
        pipeline_builder: Callable[..., Config],
    ) -> None:  # noqa: D401
        """
        Test that :class:`Config` includes profile environment variables in
        substitution when loaded from YAML.
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

    def test_from_yaml_requires_mapping_root(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that from_yaml rejects non-mapping YAML roots.
        """

        monkeypatch.setattr(config_mod.File, 'read', lambda _self: ['bad'])

        with pytest.raises(TypeError, match='mapping/object root'):
            Config.from_yaml('ignored.yml')

    def test_from_yaml_resolves_secret_tokens_incrementally(
        self,
        pipeline_builder: Callable[..., Config],
        tmp_path: Path,
    ) -> None:
        """Test config substitution can resolve environment and file secrets."""
        secrets_path = tmp_path / 'secrets.json'
        secrets_path.write_text(
            json.dumps({'service': {'password': 'file-secret'}}),
            encoding='utf-8',
        )
        yml = (
            """
name: Test
sources:
  - name: source
    type: file
    format: json
    path: "${secret:DATA_PATH}"
targets:
  - name: target
    type: api
    api: service
    endpoint: users
    headers:
      Authorization: "Bearer ${secret:file:service.password}"
jobs: []
"""
        ).strip()

        cfg = pipeline_builder(
            yml,
            env={
                'DATA_PATH': '/tmp/input.json',
                'ETLPLUS_SECRETS_FILE': str(secrets_path),
            },
        )

        source = next(item for item in cfg.sources if item.name == 'source')
        target = next(item for item in cfg.targets if item.name == 'target')
        assert getattr(source, 'path', None) == '/tmp/input.json'
        assert (
            getattr(target, 'headers', {}).get('Authorization')
            == 'Bearer file-secret'
        )

    def test_from_yaml_without_substitution_skips_token_resolution(
        self,
        pipeline_builder: Callable[..., Config],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that substitution logic is bypassed when ``substitute=False``.
        """
        monkeypatch.setattr(
            config_mod.SubstitutionResolver,
            'deep',
            lambda *_a, **_k: (_ for _ in ()).throw(
                AssertionError('deep_substitute should not be called'),
            ),
        )

        cfg = pipeline_builder(MULTI_SOURCE_YAML, substitute=False)

        s_item = next(s for s in cfg.sources if s.name == 's1')
        assert getattr(s_item, 'path', None) == '${A}-${B}.json'

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
        Test that :class:`Config` correctly handles multiple sources, targets,
        and missing variables during substitution.
        """
        path = connector_path_lookup(collection, name)
        assert path == expected_path

    def test_table_schemas_are_parsed(
        self,
        pipeline_builder: Callable[..., Config],
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
