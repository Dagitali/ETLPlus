"""
:mod:`tests.smoke.conftest` module.

Shared fixtures for smoke tests.
"""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING
from typing import Any
from typing import Protocol

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import JsonFactory

# SECTION: HELPERS ========================================================= #


@dataclass(slots=True)
class PipelineConfig:
    """Container for generated pipeline configuration paths."""

    config_path: Path
    source_path: Path
    output_path: Path
    job_name: str


@dataclass(slots=True)
class PipelineSchema:
    """Container for generated pipeline configs with table_schemas."""

    config_path: Path
    schema_name: str
    table_name: str


@dataclass(slots=True)
class TableSpec:
    """Container for generated table spec paths."""

    spec_path: Path
    schema_name: str
    table_name: str


class JsonOutputParser(Protocol):
    """Protocol for stdout JSON parsing helpers."""

    def __call__(self, output: str) -> Any: ...


class PipelineConfigFactory(Protocol):
    """Protocol for pipeline config factory fixtures."""

    def __call__(
        self,
        data: list[dict[str, Any]] | list[Any],
    ) -> PipelineConfig: ...


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(
    name='sample_records',
    params=[
        pytest.param(
            [
                {'id': 1, 'name': 'Alice'},
                {'id': 2, 'name': 'Bob'},
            ],
            id='two-records',
        ),
        pytest.param(
            [
                {'id': 99, 'name': 'Grace'},
            ],
            id='single-record',
        ),
    ],
)
def sample_records_fixture(
    request: pytest.FixtureRequest,
) -> list[dict[str, Any]]:
    """
    Return representative record payloads for smoke tests.

    Parameters
    ----------
    request : pytest.FixtureRequest
        Pytest fixture request carrying the parametrized payload.

    Returns
    -------
    list[dict[str, Any]]
        The sample record payload.
    """
    return list(request.param)


@pytest.fixture(name='sample_records_json')
def sample_records_json_fixture(
    sample_records: list[dict[str, Any]],
) -> str:
    """Return sample records serialized as JSON."""
    return json.dumps(sample_records)


@pytest.fixture(name='json_payload_file')
def json_payload_file_fixture(
    json_file_factory: JsonFactory,
    sample_records: list[dict[str, Any]],
) -> Path:
    """Persist ``sample_records`` as JSON and return the file path."""
    return json_file_factory(sample_records, filename='records.json')


@pytest.fixture(name='rules_json')
def rules_json_fixture() -> str:
    """Return simple validation rules as a JSON string."""
    rules = {
        'id': {'type': 'integer', 'min': 0},
        'name': {'type': 'string', 'minLength': 1},
    }
    return json.dumps(rules)


@pytest.fixture(name='operations_json')
def operations_json_fixture() -> str:
    """Return a basic transform operation payload as JSON."""
    return json.dumps({'select': ['id']})


@pytest.fixture(name='parse_json_output')
def parse_json_output_fixture() -> JsonOutputParser:
    """
    Parse JSON output emitted to stdout.

    Returns
    -------
    JsonOutputParser
        Callable that parses JSON strings and raises AssertionError on
        malformed output.
    """

    def _parse(output: str) -> Any:
        try:
            return json.loads(output)
        except json.JSONDecodeError as exc:
            raise AssertionError(
                f'Expected JSON output, got: {output!r}',
            ) from exc

    return _parse


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
