"""
:mod:`tests.unit.cli.test_u_cli_handlers` module.

Unit tests for :mod:`etlplus.cli._handlers`.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import ANY

import pytest

import etlplus.cli._handlers as handlers
from etlplus import Config

from .conftest import CaptureIo
from .conftest import assert_emit_json
from .conftest import assert_emit_or_write

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestCliHandlersInternalHelpers:
    """Unit tests for internal CLI helpers in :mod:`handlers`."""

    def test_check_sections_all(self, dummy_cfg: Config) -> None:
        """
        Test that :func:`_check_sections` includes all requested sections."""
        result = handlers._check_sections(
            dummy_cfg,
            jobs=False,
            pipelines=True,
            sources=True,
            targets=True,
            transforms=True,
        )
        assert set(result) >= {'pipelines', 'sources', 'targets', 'transforms'}

    def test_check_sections_default(self, dummy_cfg: Config) -> None:
        """
        Test that :func:`_check_sections` defaults to jobs when no flags are
        set.
        """
        result = handlers._check_sections(
            dummy_cfg,
            jobs=False,
            pipelines=False,
            sources=False,
            targets=False,
            transforms=False,
        )
        assert 'jobs' in result

    def test_check_sections_jobs_and_mapping_transforms(
        self,
        dummy_cfg: Config,
    ) -> None:
        """Test that jobs flag plus mapping-style transforms extraction."""
        cfg = SimpleNamespace(
            name=dummy_cfg.name,
            version=dummy_cfg.version,
            sources=dummy_cfg.sources,
            targets=dummy_cfg.targets,
            jobs=dummy_cfg.jobs,
            transforms={
                'trim': {'field': 'name'},
                'dedupe': {'on': 'id'},
            },
        )

        result = handlers._check_sections(
            cast(Config, cfg),
            jobs=True,
            pipelines=False,
            sources=False,
            targets=False,
            transforms=True,
        )
        assert result['jobs'] == ['j1']
        assert result['transforms'] == ['trim', 'dedupe']

    def test_collect_table_specs_merges_config_and_spec(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that config and standalone spec entries are merged."""
        spec_path = tmp_path / 'table.json'
        spec_path.write_text('{}', encoding='utf-8')
        monkeypatch.setattr(
            handlers,
            'load_table_spec',
            lambda _path: {'table': 'from_spec'},
        )
        monkeypatch.setattr(
            handlers.Config,
            'from_yaml',
            lambda _path, substitute: SimpleNamespace(
                table_schemas=[{'table': 'from_config'}],
            ),
        )

        specs = handlers._collect_table_specs(
            config_path='pipeline.yml',
            spec_path=str(spec_path),
        )
        assert specs == [
            {'table': 'from_spec'},
            {'table': 'from_config'},
        ]

    def test_pipeline_summary(self, dummy_cfg: Config) -> None:
        """
        Test that :func:`_pipeline_summary` returns a mapping for a pipeline
        config.
        """
        summary = handlers._pipeline_summary(dummy_cfg)
        result: Mapping[str, object] = summary
        assert result['name'] == 'p1'
        assert result['version'] == 'v1'
        assert set(result) >= {'sources', 'targets', 'jobs'}


class TestCheckHandler:
    """Unit tests for :func:`check_handler`."""

    def test_passes_substitute_flag(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`check_handler` forwards the substitute flag to config
        loader.
        """
        recorded: dict[str, object] = {}

        def fake_from_yaml(
            path: str,
            substitute: bool,
        ) -> Config:
            recorded['params'] = (path, substitute)
            return dummy_cfg

        monkeypatch.setattr(
            handlers.Config,
            'from_yaml',
            fake_from_yaml,
        )
        monkeypatch.setattr(
            handlers,
            '_check_sections',
            lambda _cfg, **_kwargs: {'pipelines': ['p1']},
        )
        assert handlers.check_handler(config='cfg.yml', substitute=True) == 0
        assert recorded['params'] == ('cfg.yml', True)
        assert_emit_json(capture_io, {'pipelines': ['p1']}, pretty=True)

    def test_prints_sections(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`check_handler` prints requested sections."""
        monkeypatch.setattr(
            handlers.Config,
            'from_yaml',
            lambda path, substitute: dummy_cfg,
        )
        monkeypatch.setattr(
            handlers,
            '_check_sections',
            lambda _cfg, **_kwargs: {'targets': ['t1']},
        )
        assert handlers.check_handler(config='cfg.yml') == 0
        assert_emit_json(capture_io, {'targets': ['t1']}, pretty=True)

    def test_summary_branch_uses_pipeline_summary(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Test that summary mode emits only the pipeline summary."""
        monkeypatch.setattr(
            handlers.Config,
            'from_yaml',
            lambda path, substitute: dummy_cfg,
        )
        monkeypatch.setattr(
            handlers,
            '_pipeline_summary',
            lambda _cfg: {'name': 'p1', 'jobs': ['j1']},
        )

        assert handlers.check_handler(config='cfg.yml', summary=True) == 0
        assert_emit_json(
            capture_io,
            {'name': 'p1', 'jobs': ['j1']},
            pretty=True,
        )


class TestExtractHandler:
    """Unit tests for :func:`extract_handler`."""

    def test_calls_extract_for_non_file_sources(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`extract_handler` uses extract for non-STDIN sources.
        """
        observed: dict[str, object] = {}

        def fake_extract(
            source_type: str,
            source: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            observed['params'] = (source_type, source, file_format)
            return {'status': 'ok'}

        monkeypatch.setattr(handlers, 'extract', fake_extract)

        assert (
            handlers.extract_handler(
                source_type='api',
                source='endpoint',
                format_hint='json',
                format_explicit=True,
                output=None,
                pretty=True,
            )
            == 0
        )

        assert observed['params'] == ('api', 'endpoint', 'json')
        kwargs = assert_emit_or_write(
            capture_io,
            {'status': 'ok'},
            None,
            pretty=True,
        )
        assert kwargs['success_message'] == ANY

    def test_file_respects_explicit_format(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`extract_handler` forwards explicit file format hints.
        """
        captured: dict[str, object] = {}

        def fake_extract(
            source_type: str,
            source: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            captured['params'] = (source_type, source, file_format)
            return {'ok': True}

        monkeypatch.setattr(handlers, 'extract', fake_extract)
        assert (
            handlers.extract_handler(
                source_type='file',
                source='table.dat',
                format_hint='csv',
                format_explicit=True,
                output=None,
                pretty=True,
            )
            == 0
        )
        assert captured['params'] == ('file', 'table.dat', 'csv')
        assert len(capture_io['emit_or_write']) == 1

    def test_reads_stdin_and_emits_json(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`extract_handler` reads STDIN and emits parsed data.
        """
        monkeypatch.setattr(
            handlers.cli_io,
            'read_stdin_text',
            lambda: 'raw-text',
        )
        monkeypatch.setattr(
            handlers.cli_io,
            'parse_text_payload',
            lambda text, fmt: {'payload': text, 'fmt': fmt},
        )

        def fail_extract(*_args: object, **_kwargs: object) -> None:
            raise AssertionError('extract should not be called')

        monkeypatch.setattr(handlers, 'extract', fail_extract)
        assert (
            handlers.extract_handler(
                source_type='api',
                source='-',
                format_hint=None,
                format_explicit=False,
                output=None,
                pretty=False,
            )
            == 0
        )
        assert_emit_json(
            capture_io,
            {'payload': 'raw-text', 'fmt': None},
            pretty=False,
        )
        assert capture_io['emit_or_write'] == []

    def test_target_argument_overrides_output_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that ``target`` takes precedence over ``output`` when both are
        set.
        """
        monkeypatch.setattr(
            handlers,
            'extract',
            lambda *_args, **_kwargs: {'status': 'ok'},
        )

        assert (
            handlers.extract_handler(
                source_type='file',
                source='data.json',
                target='preferred.json',
                output='ignored.json',
                format_hint='json',
                format_explicit=True,
                pretty=False,
            )
            == 0
        )
        assert_emit_or_write(
            capture_io,
            {'status': 'ok'},
            'preferred.json',
            pretty=False,
        )

    def test_writes_output_file_and_skips_emit(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`extract_handler` writes to a file and skips STDOUT
        emission.
        """
        observed: dict[str, object] = {}

        def fake_extract(
            source_type: str,
            source: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            observed['params'] = (source_type, source, file_format)
            return {'status': 'ok'}

        monkeypatch.setattr(handlers, 'extract', fake_extract)

        assert (
            handlers.extract_handler(
                source_type='api',
                source='endpoint',
                target='export.json',
                format_hint='json',
                format_explicit=True,
                pretty=True,
            )
            == 0
        )
        assert observed['params'] == ('api', 'endpoint', 'json')
        kwargs = assert_emit_or_write(
            capture_io,
            {'status': 'ok'},
            'export.json',
            pretty=True,
        )
        assert isinstance(kwargs['success_message'], str)


class TestLoadHandler:
    """Unit tests for :func:`load_handler`."""

    def test_file_target_streams_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`load_handler` streams payload for file targets."""
        recorded: dict[str, object] = {}

        def fake_materialize(
            src: str,
            *,
            format_hint: str | None,
            format_explicit: bool,
        ) -> list[object]:
            recorded['call'] = (src, format_hint, format_explicit)
            return ['rows', src]

        monkeypatch.setattr(
            handlers.cli_io,
            'materialize_file_payload',
            fake_materialize,
        )

        def fail_load(*_args: object, **_kwargs: object) -> None:
            raise AssertionError('load should not be called for STDOUT path')

        monkeypatch.setattr(handlers, 'load', fail_load)

        assert (
            handlers.load_handler(
                source='data.csv',
                target_type='file',
                target='-',
                source_format=None,
                target_format=None,
                format_explicit=False,
                output=None,
                pretty=True,
            )
            == 0
        )
        assert recorded['call'] == ('data.csv', None, False)
        assert_emit_json(capture_io, ['rows', 'data.csv'], pretty=True)

    def test_reads_stdin_and_invokes_load(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`load_handler` parses STDIN and routes through load.
        """
        read_calls = {'count': 0}

        def fake_read_stdin() -> str:
            read_calls['count'] += 1
            return 'stdin-payload'

        monkeypatch.setattr(
            handlers.cli_io,
            'read_stdin_text',
            fake_read_stdin,
        )

        parsed_payload = {'payload': 'stdin-payload', 'fmt': None}
        parse_calls: dict[str, object] = {}

        def fake_parse(text: str, fmt: str | None) -> object:
            parse_calls['params'] = (text, fmt)
            return parsed_payload

        monkeypatch.setattr(handlers.cli_io, 'parse_text_payload', fake_parse)

        def fail_materialize(*_args: object, **_kwargs: object) -> None:
            raise AssertionError(
                'materialize_file_payload should not be called for STDIN sources',
            )

        monkeypatch.setattr(
            handlers.cli_io,
            'materialize_file_payload',
            fail_materialize,
        )

        load_record: dict[str, object] = {}

        def fake_load(
            payload: object,
            target_type: str,
            target: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            load_record['params'] = (payload, target_type, target, file_format)
            return {'loaded': True}

        monkeypatch.setattr(handlers, 'load', fake_load)

        assert (
            handlers.load_handler(
                source='-',
                target_type='api',
                target='endpoint',
                source_format=None,
                target_format=None,
                format_explicit=False,
                output=None,
                pretty=False,
            )
            == 0
        )
        assert read_calls['count'] == 1
        assert parse_calls['params'] == ('stdin-payload', None)
        assert load_record['params'] == (
            parsed_payload,
            'api',
            'endpoint',
            None,
        )
        kwargs = assert_emit_or_write(
            capture_io,
            {'loaded': True},
            None,
            pretty=False,
        )
        assert isinstance(kwargs['success_message'], str)

    def test_writes_output_file_and_skips_emit(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`load_handler` writes to a file and skips STDOUT
        emission.
        """
        load_record: dict[str, object] = {}

        def fake_load(
            payload_obj: object,
            target_type: str,
            target: str,
            *,
            file_format: str | None,
        ) -> dict[str, object]:
            load_record['params'] = (
                payload_obj,
                target_type,
                target,
                file_format,
            )
            return {'status': 'queued'}

        monkeypatch.setattr(handlers, 'load', fake_load)

        assert (
            handlers.load_handler(
                source='payload.json',
                target_type='db',
                target='warehouse',
                source_format='json',
                target_format='json',
                format_explicit=True,
                output='result.json',
                pretty=True,
            )
            == 0
        )
        assert load_record['params'] == (
            'payload.json',
            'db',
            'warehouse',
            'json',
        )
        kwargs = assert_emit_or_write(
            capture_io,
            {'status': 'queued'},
            'result.json',
            pretty=True,
        )
        assert isinstance(kwargs['success_message'], str)


class TestRenderHandler:
    """Unit tests for :func:`render_handler`."""

    def test_errors_without_specs(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`render_handler` reports missing specs."""

        assert (
            handlers.render_handler(
                config=None,
                spec=None,
                table=None,
                template='ddl',
                template_path=None,
                output=None,
                pretty=True,
                quiet=False,
            )
            == 1
        )
        assert 'No table schemas found' in capsys.readouterr().err

    def test_output_file_respects_quiet_flag(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test output-file rendering without a status log in quiet mode."""
        output_path = tmp_path / 'rendered.sql'
        monkeypatch.setattr(
            handlers,
            '_collect_table_specs',
            lambda _cfg, _spec: [{'table': 'Widget'}],
        )
        monkeypatch.setattr(
            handlers,
            'render_tables',
            lambda specs, **kwargs: ['SELECT 1'],
        )

        assert (
            handlers.render_handler(
                config='pipeline.yml',
                spec=None,
                table=None,
                template='ddl',
                template_path='custom.sql.j2',
                output=str(output_path),
                pretty=True,
                quiet=True,
            )
            == 0
        )
        assert output_path.read_text(encoding='utf-8') == 'SELECT 1\n'
        assert 'Rendered 1 schema(s)' not in capsys.readouterr().out

    def test_uses_template_file_override_when_template_is_a_path(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """
        Test that template-path auto-detection from the ``template`` argument.
        """
        template_path = tmp_path / 'ddl.sql.j2'
        template_path.write_text('CREATE TABLE {{ table }}', encoding='utf-8')
        monkeypatch.setattr(
            handlers,
            '_collect_table_specs',
            lambda _cfg, _spec: [{'table': 'Widget'}],
        )
        captured: dict[str, object] = {}

        def _render_tables(
            specs: list[dict[str, object]],
            *,
            template: str | None,
            template_path: str | None,
        ) -> list[str]:
            captured['specs'] = specs
            captured['template'] = template
            captured['template_path'] = template_path
            return ['SELECT 1']

        monkeypatch.setattr(handlers, 'render_tables', _render_tables)

        assert (
            handlers.render_handler(
                config='pipeline.yml',
                spec=None,
                table='Widget',
                template=cast(Any, str(template_path)),
                template_path=None,
                output='-',
                pretty=False,
                quiet=True,
            )
            == 0
        )
        assert captured['template'] is None
        assert captured['template_path'] == str(template_path)
        assert captured['specs'] == [{'table': 'Widget'}]
        assert capsys.readouterr().out.strip() == 'SELECT 1'

    def test_writes_sql_from_spec(
        self,
        widget_spec_paths: tuple[Path, Path],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`render_handler` writes SQL for standalone specs."""
        spec_path, output_path = widget_spec_paths
        assert (
            handlers.render_handler(
                config=None,
                spec=str(spec_path),
                table=None,
                template='ddl',
                template_path=None,
                output=str(output_path),
                pretty=True,
                quiet=False,
            )
            == 0
        )

        sql_text = output_path.read_text(encoding='utf-8')
        assert 'CREATE TABLE [dbo].[Widget]' in sql_text

        captured = capsys.readouterr()
        assert f'Rendered 1 schema(s) to {output_path}' in captured.out


class TestRunHandler:
    """Unit tests for :func:`run_handler`."""

    def test_emits_pipeline_summary_without_job(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`run_handler` emits a summary when no job set."""
        monkeypatch.setattr(
            handlers.Config,
            'from_yaml',
            lambda path, substitute: dummy_cfg,
        )

        assert (
            handlers.run_handler(
                config='pipeline.yml',
                job=None,
                pipeline=None,
                pretty=True,
            )
            == 0
        )

        assert_emit_json(
            capture_io,
            {
                'name': dummy_cfg.name,
                'version': dummy_cfg.version,
                'sources': ['s1'],
                'targets': ['t1'],
                'jobs': ['j1'],
            },
            pretty=True,
        )

    def test_runs_job_and_emits_result(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: Config,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`run_handler` executes a named job and emits status.
        """
        monkeypatch.setattr(
            handlers.Config,
            'from_yaml',
            lambda path, substitute: dummy_cfg,
        )
        run_calls: dict[str, object] = {}

        def fake_run(*, job: str, config_path: str) -> dict[str, object]:
            run_calls['params'] = (job, config_path)
            return {'job': job, 'ok': True}

        monkeypatch.setattr(handlers, 'run', fake_run)

        assert (
            handlers.run_handler(
                config='pipeline.yml',
                job='job1',
                pretty=False,
            )
            == 0
        )
        assert run_calls['params'] == ('job1', 'pipeline.yml')
        assert_emit_json(
            capture_io,
            {'status': 'ok', 'result': {'job': 'job1', 'ok': True}},
            pretty=False,
        )


class TestTransformHandler:
    """Unit tests for :func:`transform_handler`."""

    def test_emits_result_without_target(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`transform_handler` emits results with no target."""
        resolve_calls: list[tuple[object, str | None, bool]] = []

        def fake_resolve(
            source: object,
            *,
            format_hint: str | None,
            format_explicit: bool,
        ) -> object:
            resolve_calls.append((source, format_hint, format_explicit))
            if source == 'data.json':
                return [{'id': 1}]
            return {'select': ['id']}

        monkeypatch.setattr(
            handlers.cli_io,
            'resolve_cli_payload',
            fake_resolve,
        )
        monkeypatch.setattr(
            handlers,
            'transform',
            lambda payload, ops: {'rows': payload, 'ops': ops},
        )

        assert (
            handlers.transform_handler(
                source='data.json',
                operations='ops.json',
                source_format='json',
                target=None,
                pretty=False,
            )
            == 0
        )
        assert resolve_calls == [
            ('data.json', 'json', True),
            ('ops.json', None, True),
        ]
        assert_emit_json(
            capture_io,
            {'rows': [{'id': 1}], 'ops': {'select': ['id']}},
            pretty=False,
        )

    def test_requires_mapping_operations_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that non-mapping operations payloads raise :class:`ValueError`.
        """

        def _resolve_cli_payload(
            source: object,
            **kwargs: object,
        ) -> object:
            if source == 'ops.json':
                return ['not-a-mapping']
            return [{'id': 1}]

        monkeypatch.setattr(
            handlers.cli_io,
            'resolve_cli_payload',
            _resolve_cli_payload,
        )
        with pytest.raises(ValueError, match='operations must resolve'):
            handlers.transform_handler(
                source='data.json',
                operations='ops.json',
                source_format='json',
                target=None,
                pretty=True,
            )

    def test_writes_remote_target_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`transform_handler` preserves remote URI targets."""
        monkeypatch.setattr(
            handlers.cli_io,
            'resolve_cli_payload',
            lambda source, **_kwargs: (
                {'source': source} if source == 'data.json' else {'select': ['id']}
            ),
        )
        monkeypatch.setattr(
            handlers,
            'transform',
            lambda payload, ops: {'payload': payload, 'ops': ops},
        )
        write_calls: dict[str, object] = {}

        def fake_write(self, data, **kwargs):
            write_calls['params'] = (self.path, data)

        monkeypatch.setattr(handlers.File, 'write', fake_write)

        assert (
            handlers.transform_handler(
                source='data.json',
                operations='ops.json',
                target='s3://bucket/out.json',
                target_format='json',
                pretty=True,
            )
            == 0
        )
        assert write_calls['params'] == (
            's3://bucket/out.json',
            {
                'payload': {'source': 'data.json'},
                'ops': {'select': ['id']},
            },
        )
        assert (
            'Data transformed and saved to s3://bucket/out.json'
            in capsys.readouterr().out
        )

    def test_writes_target_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`transform_handler` writes data to a target file."""
        monkeypatch.setattr(
            handlers.cli_io,
            'resolve_cli_payload',
            lambda source, **_kwargs: (
                {'source': source} if source == 'data.json' else {'select': ['id']}
            ),
        )
        monkeypatch.setattr(
            handlers,
            'transform',
            lambda payload, ops: {'payload': payload, 'ops': ops},
        )
        write_calls: dict[str, object] = {}

        def fake_write(self, data, **kwargs):
            # Only capture path and data; ignore root_tag.
            write_calls['params'] = (str(self.path), data)

        monkeypatch.setattr(handlers.File, 'write', fake_write)

        assert (
            handlers.transform_handler(
                source='data.json',
                operations='ops.json',
                target='out.json',
                target_format='json',
                pretty=True,
            )
            == 0
        )
        assert write_calls['params'] == (
            'out.json',
            {
                'payload': {'source': 'data.json'},
                'ops': {'select': ['id']},
            },
        )
        assert 'Data transformed and saved to out.json' in capsys.readouterr().out


class TestValidateHandler:
    """Unit tests for :func:`validate_handler`."""

    def test_emits_result_without_target(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`validate_handler` emits results with no target."""
        monkeypatch.setattr(
            handlers.cli_io,
            'resolve_cli_payload',
            lambda source, **_kwargs: (
                {'source': source}
                if source == 'data.json'
                else {'id': {'required': True}}
            ),
        )
        monkeypatch.setattr(
            handlers,
            'validate',
            lambda payload, rules: {'data': payload, 'rules': rules},
        )

        assert (
            handlers.validate_handler(
                source='data.json',
                rules='rules.json',
                pretty=False,
            )
            == 0
        )
        assert_emit_json(
            capture_io,
            {
                'data': {'source': 'data.json'},
                'rules': {'id': {'required': True}},
            },
            pretty=False,
        )

    def test_reports_missing_data_for_target(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`validate_handler` reports missing output data."""
        monkeypatch.setattr(
            handlers.cli_io,
            'resolve_cli_payload',
            lambda source, **_kwargs: (
                {'source': source}
                if source == 'data.json'
                else {'id': {'required': True}}
            ),
        )
        monkeypatch.setattr(
            handlers,
            'validate',
            lambda *_args, **_kwargs: {'data': None},
        )

        assert (
            handlers.validate_handler(
                source='data.json',
                rules='rules.json',
                target='out.json',
                pretty=True,
            )
            == 0
        )
        assert (
            'ValidationDict failed, no data to save for out.json'
            in capsys.readouterr().err
        )

    def test_requires_mapping_rules_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that non-mapping rules payloads raise :class:`ValueError`.
        """

        def _resolve_cli_payload(
            source: object,
            **kwargs: object,
        ) -> object:
            if source == 'rules.json':
                return ['not-a-mapping']
            return [{'id': 1}]

        monkeypatch.setattr(
            handlers.cli_io,
            'resolve_cli_payload',
            _resolve_cli_payload,
        )
        with pytest.raises(ValueError, match='rules must resolve'):
            handlers.validate_handler(
                source='data.json',
                rules='rules.json',
                source_format='json',
                target=None,
                pretty=True,
            )

    def test_target_stdout_emits_result_json(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that ``target='-'`` emits full validation output to STDOUT.
        """
        monkeypatch.setattr(
            handlers.cli_io,
            'resolve_cli_payload',
            lambda source, **_kwargs: (
                {'source': source}
                if source == 'data.json'
                else {'id': {'required': True}}
            ),
        )
        monkeypatch.setattr(
            handlers,
            'validate',
            lambda payload, rules: {
                'data': payload,
                'field_errors': {},
                'rules': rules,
                'valid': True,
            },
        )

        assert (
            handlers.validate_handler(
                source='data.json',
                rules='rules.json',
                target='-',
                pretty=True,
            )
            == 0
        )
        assert_emit_json(
            capture_io,
            {
                'data': {'source': 'data.json'},
                'field_errors': {},
                'rules': {'id': {'required': True}},
                'valid': True,
            },
            pretty=True,
        )

    def test_writes_target_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`validate_handler` writes data to a target file."""
        monkeypatch.setattr(
            handlers.cli_io,
            'resolve_cli_payload',
            lambda source, **_kwargs: (
                {'source': source}
                if source == 'data.json'
                else {'id': {'required': True}}
            ),
        )
        monkeypatch.setattr(
            handlers,
            'validate',
            lambda *_args, **_kwargs: {'data': {'id': 1}},
        )
        write_calls: dict[str, object] = {}

        def fake_write(
            data: object,
            path: str | None,
            *,
            success_message: str,
        ) -> bool:
            write_calls['params'] = (data, path, success_message)
            return True

        monkeypatch.setattr(handlers.cli_io, 'write_json_output', fake_write)

        assert (
            handlers.validate_handler(
                source='data.json',
                rules='rules.json',
                target='out.json',
                pretty=True,
            )
            == 0
        )
        assert write_calls['params'] == (
            {'id': 1},
            'out.json',
            'ValidationDict result saved to',
        )
