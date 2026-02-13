"""
:mod:`tests.smoke.conftest` module.

Define shared fixtures and helpers for pytest-based smoke tests of
:mod:`etlplus`.

Notes
-----
- Fixtures are designed for reuse and DRY test setup.
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

from etlplus.file.base import WriteOptions

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import JsonFactory

# SECTION: MARKERS ========================================================== #


# Directory-level marker for smoke tests.
pytestmark = pytest.mark.smoke


# SECTION: TYPES ============================================================ #


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


class FileModule(Protocol):
    """Protocol for file format modules exposing a singleton handler."""

    __name__: str


class PipelineConfigFactory(Protocol):
    """Protocol for pipeline config factory fixtures."""

    def __call__(
        self,
        data: list[dict[str, Any]] | list[Any],
    ) -> PipelineConfig: ...


class SmokeRoundtripModuleContract:
    """Reusable write/read smoke contract for file-format modules."""

    module: FileModule
    file_name: str
    payload: object | None = None
    use_sample_record: bool = False
    write_kwargs: dict[str, object] | None = None
    expect_write_error: type[Exception] | None = None
    error_match: str | None = None

    def build_payload(
        self,
        *,
        sample_record: dict[str, Any],
        sample_records: list[dict[str, Any]],
    ) -> object:
        """Return the roundtrip payload for one smoke test module."""
        if self.payload is not None:
            return self.payload
        if self.use_sample_record:
            return sample_record
        return sample_records

    def test_roundtrip_smoke(
        self,
        tmp_path: Path,
        sample_record: dict[str, Any],
        sample_records: list[dict[str, Any]],
    ) -> None:
        """Test that read/write can be invoked with minimal payloads."""
        path = tmp_path / self.file_name
        payload = self.build_payload(
            sample_record=sample_record,
            sample_records=sample_records,
        )
        run_file_smoke(
            self.module,
            path,
            payload,
            write_kwargs=self.write_kwargs,
            expect_write_error=self.expect_write_error,
            error_match=self.error_match,
        )


# SECTION: FIXTURES ========================================================= #


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


# SECTION: FUNCTIONS ======================================================== #


def run_file_smoke(
    module: FileModule,
    path: Path,
    payload: object,
    *,
    write_kwargs: dict[str, object] | None = None,
    expect_write_error: type[Exception] | None = None,
    error_match: str | None = None,
) -> None:
    """
    Run a minimal read/write smoke cycle for file modules.

    Parameters
    ----------
    module : FileModule
        File module exposing ``read``/``write`` functions.
    path : Path
        Target path for the test file.
    payload : object
        Payload passed to ``write``.
    write_kwargs : dict[str, object] | None, optional
        Keyword arguments forwarded to ``write``.
    expect_write_error : type[Exception] | None, optional
        Expected exception type for write failures.
    error_match : str | None, optional
        Regex message to assert when ``expect_write_error`` is provided.

    Raises
    ------
    OSError
        If the file operation fails due to OS-level issues (e.g., permissions).
    TypeError
        If the payload is of an unexpected type for the file format.
    """
    write_kwargs = dict(write_kwargs or {})
    handlers = [
        value
        for name, value in vars(module).items()
        if name.endswith('_HANDLER')
    ]
    assert len(handlers) == 1
    handler = handlers[0]

    if 'root_tag' in write_kwargs and 'options' not in write_kwargs:
        root_tag = write_kwargs.pop('root_tag')
        if not isinstance(root_tag, str):
            raise TypeError('root_tag must be a string')
        write_kwargs['options'] = WriteOptions(
            root_tag=root_tag,
        )
    try:
        if expect_write_error is not None:
            match = error_match or ''
            with pytest.raises(expect_write_error, match=match):
                handler.write(path, payload, **write_kwargs)
            return
        written = handler.write(path, payload, **write_kwargs)
        assert written
        result = handler.read(path)
        assert result
    except OSError as exc:
        if (
            module.__name__.endswith('.orc')
            and 'sysctlbyname' in str(exc)
        ):
            pytest.skip('ORC read failed due to sysctl limitations')
        raise
    except ImportError as exc:
        pytest.skip(str(exc))
