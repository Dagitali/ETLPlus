"""
:mod:`tests.unit.database.test_u_database_ddl` module.

Unit tests for :mod:`etlplus.database.ddl`.
"""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from etlplus.database import ddl

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='sample_spec')
def fixture_sample_spec() -> dict[str, object]:
    """Sample table specification for testing."""
    return {
        'schema': 'dbo',
        'table': 'widgets',
        'create_schema': False,
        'columns': [
            {
                'name': 'id',
                'type': 'INT',
                'nullable': False,
                'identity': {'seed': 1, 'increment': 1},
            },
            {
                'name': 'name',
                'type': 'NVARCHAR(255)',
                'nullable': True,
            },
        ],
        'primary_key': {
            'columns': ['id'],
        },
        'indexes': [
            {
                'name': 'IX_widgets_name',
                'columns': ['name'],
                'unique': True,
            },
        ],
        'foreign_keys': [],
    }


# SECTION: TESTS ============================================================ #


class TestLoadTableSpec:
    """Unit test suite for :func:`load_table_spec`."""

    def test_missing_yaml_dependency(
        self,
        tmp_path: Path,
        sample_spec: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that loading a YAML spec without PyYAML raises RuntimeError."""
        yaml = pytest.importorskip('yaml')
        spec_path = tmp_path / 'spec.yaml'
        spec_path.write_text(
            yaml.safe_dump(sample_spec, sort_keys=False),
            encoding='utf-8',
        )
        # pylint: disable=import-outside-toplevel,protected-access

        import etlplus.file._imports as import_helpers
        import etlplus.file.yaml as file_mod

        import_helpers._MODULE_CACHE.clear()

        def _raise_import_error() -> None:
            raise ImportError('forced failure for test')

        monkeypatch.setattr(file_mod, 'get_yaml', _raise_import_error)

        with pytest.raises(RuntimeError):
            ddl.load_table_spec(spec_path)

    def test_requires_mapping(self, tmp_path: Path) -> None:
        """Test that loading a spec requires a mapping at the top level."""
        spec_path = tmp_path / 'array.json'
        spec_path.write_text(
            json.dumps([{'not': 'mapping'}]),
            encoding='utf-8',
        )
        with pytest.raises(TypeError):
            ddl.load_table_spec(spec_path)

    @pytest.mark.parametrize(
        'extension',
        ['json', 'yaml'],
        ids=['json', 'yaml'],
    )
    def test_roundtrip(
        self,
        tmp_path: Path,
        extension: str,
        sample_spec: dict[str, object],
    ) -> None:
        """Test loading a table spec from JSON and YAML formats."""
        spec_path = tmp_path / f'spec.{extension}'
        materialized = deepcopy(sample_spec)
        if extension == 'json':
            spec_path.write_text(json.dumps(materialized), encoding='utf-8')
        else:
            yaml = pytest.importorskip('yaml')
            spec_path.write_text(
                yaml.safe_dump(materialized, sort_keys=False),
                encoding='utf-8',
            )

        loaded = ddl.load_table_spec(spec_path)
        assert loaded == materialized

    def test_unsupported_suffix(self, tmp_path: Path) -> None:
        """
        Test that loading a spec with an unsupported suffix raises ValueError.
        """
        spec_path = tmp_path / 'spec.txt'
        spec_path.write_text('{}', encoding='utf-8')

        with pytest.raises(ValueError):
            ddl.load_table_spec(spec_path)


class TestRenderTableSql:
    """Unit test suite for :func:`render_table_sql`."""

    def test_custom_template_path(
        self,
        tmp_path: Path,
        sample_spec: dict[str, object],
    ) -> None:
        """Test rendering SQL with a custom template path."""
        template_path = tmp_path / 'custom.sql.j2'
        template_path.write_text('{{ spec.table }}', encoding='utf-8')

        sql = ddl.render_table_sql(
            sample_spec,
            template_path=str(template_path),
        )

        assert sql == f'{sample_spec["table"]}\n'

    def test_default_template(
        self,
        sample_spec: dict[str, object],
    ) -> None:
        """Test rendering SQL with the default template."""
        sql = ddl.render_table_sql(sample_spec)
        assert f'CREATE TABLE [dbo].[{sample_spec["table"]}' in sql
        assert '[id] INT' in sql

    def test_env_override(
        self,
        tmp_path: Path,
        sample_spec: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test rendering SQL with an environment variable override for the
        template path.
        """
        template_path = tmp_path / 'env_template.sql.j2'
        template_path.write_text(
            '{{ spec.schema }}.{{ spec.table }}',
            encoding='utf-8',
        )
        monkeypatch.setenv('TEMPLATE_NAME', str(template_path))

        sql = ddl.render_table_sql(sample_spec, template=None)

        assert sql == 'dbo.widgets\n'

    def test_missing_template_path(
        self,
        sample_spec: dict[str, object],
    ) -> None:
        """Test that a missing template path raises FileNotFoundError."""
        missing = Path('/nonexistent/template.sql.j2')
        with pytest.raises(FileNotFoundError):
            ddl.render_table_sql(sample_spec, template_path=str(missing))

    def test_unknown_template_key(
        self,
        sample_spec: dict[str, object],
    ) -> None:
        """Test that an unknown template key raises ValueError."""
        with pytest.raises(ValueError):
            ddl.render_table_sql(
                sample_spec,
                template='does not exist',  # type: ignore[arg-type]
            )


class TestRenderTablesToString:
    """
    Unit test suite for :func:`render_tables_to_string`.
    """

    def test_custom_template(
        self,
        tmp_path: Path,
        sample_spec: dict[str, object],
    ) -> None:
        """
        Test rendering multiple table specs to a string with a custom template.
        """
        template_path = tmp_path / 'concat_template.sql.j2'
        template_path.write_text('{{ spec.table }}', encoding='utf-8')

        path = tmp_path / 'spec.json'
        path.write_text(json.dumps(sample_spec), encoding='utf-8')

        sql = ddl.render_tables_to_string(
            [path],
            template_path=template_path,
        )

        assert sql == f'{sample_spec["table"]}\n'

    def test_from_paths(
        self,
        tmp_path: Path,
        sample_spec: dict[str, object],
    ) -> None:
        """
        Test rendering multiple table specs from file paths into a single SQL
        string.
        """
        spec_paths: list[Path] = []
        for idx, table_name in enumerate(('widgets', 'widgets_history')):
            materialized = deepcopy(sample_spec)
            materialized['table'] = table_name
            path = tmp_path / f'spec_{idx}.json'
            path.write_text(json.dumps(materialized), encoding='utf-8')
            spec_paths.append(path)

        sql = ddl.render_tables_to_string(spec_paths)

        assert 'widgets' in sql
        assert 'widgets_history' in sql

    def test_templates_constant_exposes_builtin_keys(self) -> None:
        """Test that TEMPLATES constant includes expected built-in keys."""
        assert {'ddl', 'view'}.issubset(set(ddl.TEMPLATES))


class TestTemplate:
    """Unit test suite for ``TEMPLATE``."""

    def test_builtin_keys_exposure(self) -> None:
        """Test that TEMPLATES constant includes expected built-in keys."""
        assert {'ddl', 'view'}.issubset(set(ddl.TEMPLATES))
