"""
:mod:`tests.integration.cli.conftest` module.

Shared fixtures and helpers for pytest-based integration tests of
:mod:`etlplus.cli` modules.
"""

from __future__ import annotations

import itertools
import json
from collections.abc import Iterator
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING
from typing import Any
from uuid import uuid4

import pytest

from etlplus.storage import StorageLocation
from etlplus.storage import get_backend
from tests.integration.cli.pytest_cli_integration_support import PipelineConfig
from tests.integration.cli.pytest_cli_integration_support import PipelineConfigFactory
from tests.integration.cli.pytest_cli_integration_support import PipelineSchema
from tests.integration.cli.pytest_cli_integration_support import RealRemoteSource
from tests.integration.cli.pytest_cli_integration_support import RealRemoteSourceFactory
from tests.integration.cli.pytest_cli_integration_support import RealRemoteTarget
from tests.integration.cli.pytest_cli_integration_support import RealRemoteTargetFactory
from tests.integration.cli.pytest_cli_integration_support import TableSpec
from tests.integration.pytest_integration_support import child_uri
from tests.integration.pytest_integration_support import require_env

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import JsonFactory

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: MARKERS ========================================================== #


# Directory-level marker for integration tests.
pytestmark = [pytest.mark.integration, pytest.mark.smoke]


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(autouse=True)
def isolated_cli_state_dir_fixture(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Route CLI state/history writes to a per-test temporary directory."""
    state_dir = tmp_path / '.etlplus-state'
    state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv('ETLPLUS_STATE_DIR', str(state_dir))


@pytest.fixture(name='pipeline_config_factory')
def pipeline_config_factory_fixture(
    tmp_path: Path,
    json_file_factory: JsonFactory,
) -> PipelineConfigFactory:
    """
    Build minimal pipeline YAML files for smoke tests.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory managed by pytest.
    json_file_factory : JsonFactory
        Factory for JSON input payloads.

    Returns
    -------
    PipelineConfigFactory
        Callable that returns a prepared pipeline config bundle.
    """
    counter = itertools.count(1)

    def _build(
        data: list[dict[str, Any]] | list[Any],
    ) -> PipelineConfig:
        idx = next(counter)
        source_path = json_file_factory(data, filename=f'input_{idx}.json')
        output_path = tmp_path / f'output_{idx}.json'
        job_name = f'file_to_file_smoke_{idx}'
        pipeline_yaml = dedent(
            f"""
            name: Smoke Test
            sources:
              - name: src
                type: file
                format: json
                path: "{source_path}"
            targets:
              - name: dest
                type: file
                format: json
                path: "{output_path}"
            jobs:
              - name: {job_name}
                extract:
                  source: src
                load:
                  target: dest
            """,
        ).strip()
        cfg_path = tmp_path / f'pipeline_{idx}.yml'
        cfg_path.write_text(pipeline_yaml, encoding='utf-8')
        return PipelineConfig(
            config_path=cfg_path,
            source_path=source_path,
            output_path=output_path,
            job_name=job_name,
        )

    return _build


@pytest.fixture(name='pipeline_table_schemas_config')
def pipeline_table_schemas_config_fixture(
    tmp_path: Path,
) -> PipelineSchema:
    """
    Create a pipeline YAML containing ``table_schemas`` for render smoke tests.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory managed by pytest.

    Returns
    -------
    PipelineSchema
        Bundle containing the pipeline path and table identifiers.
    """
    schema_name = 'dbo'
    table_name = 'SmokePipelineUsers'
    pipeline_yaml = dedent(
        f"""
        name: Smoke Render Pipeline
        table_schemas:
          - schema: {schema_name}
            table: {table_name}
            columns:
              - name: id
                type: int
                nullable: false
              - name: name
                type: nvarchar(100)
                nullable: false
            primary_key:
              columns: [id]
        """,
    ).strip()
    cfg_path = tmp_path / 'pipeline_table_schemas.yml'
    cfg_path.write_text(pipeline_yaml, encoding='utf-8')
    return PipelineSchema(
        config_path=cfg_path,
        schema_name=schema_name,
        table_name=table_name,
    )


@pytest.fixture(name='real_remote_source_factory')
def real_remote_source_factory_fixture(
    real_remote_target_factory: RealRemoteTargetFactory,
) -> RealRemoteSourceFactory:
    """Provision and seed env-gated real cloud source URIs for tests."""

    def _build(
        env_name: str,
        *,
        payload: Any,
        suffix: str,
        file_format: str = 'json',
    ) -> RealRemoteSource:
        target = real_remote_target_factory(
            env_name,
            suffix=suffix,
            extension=file_format,
        )
        content = json.dumps(payload)
        with target.backend.open(target.location, mode='wb') as handle:
            handle.write(content.encode('utf-8'))
        return RealRemoteSource(
            uri=target.uri,
            location=target.location,
            backend=target.backend,
        )

    return _build


@pytest.fixture(name='real_remote_target_factory')
def real_remote_target_factory_fixture() -> Iterator[RealRemoteTargetFactory]:
    """Provision env-gated real cloud target URIs and clean them up."""
    created: list[RealRemoteTarget] = []

    def _build(
        env_name: str,
        *,
        suffix: str,
        extension: str = 'json',
    ) -> RealRemoteTarget:
        base_uri = require_env(env_name)
        uri = child_uri(
            base_uri,
            f'etlplus-cli-{suffix}-{uuid4().hex}.{extension}',
        )
        location = StorageLocation.from_value(uri)
        target = RealRemoteTarget(
            uri=uri,
            location=location,
            backend=get_backend(location),
        )
        created.append(target)
        return target

    yield _build

    for target in reversed(created):
        if target.backend.exists(target.location):
            target.backend.delete(target.location)


@pytest.fixture(name='table_spec')
def table_spec_fixture(
    tmp_path: Path,
) -> TableSpec:
    """
    Create a minimal table spec JSON file for render smoke tests.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory managed by pytest.

    Returns
    -------
    TableSpec
        Bundle containing the spec file path and identifiers.
    """
    schema_name = 'dbo'
    table_name = 'SmokeUsers'
    spec = {
        'schema': schema_name,
        'table': table_name,
        'columns': [
            {'name': 'id', 'type': 'int', 'nullable': False},
            {'name': 'name', 'type': 'nvarchar(100)', 'nullable': False},
        ],
        'primary_key': {'columns': ['id']},
    }
    spec_path = tmp_path / 'table_spec.json'
    spec_path.write_text(json.dumps(spec, indent=2), encoding='utf-8')
    return TableSpec(
        spec_path=spec_path,
        schema_name=schema_name,
        table_name=table_name,
    )
