"""
:mod:`tests.unit.cli.test_u_cli_handlers` module.

Unit tests for :mod:`etlplus.cli.handlers`.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from unittest.mock import ANY

import pytest

import etlplus.cli.handlers as handlers
from etlplus.workflow import PipelineConfig
from tests.unit.cli.conftest import CaptureHandler
from tests.unit.cli.conftest import CaptureIo
from tests.unit.cli.conftest import assert_emit_json
from tests.unit.cli.conftest import assert_emit_or_write

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


class TestCliHandlersInternalHelpers:
    """Unit tests for internal CLI helpers in :mod:`etlplus.cli.handlers`."""

    def test_check_sections_all(self, dummy_cfg: PipelineConfig) -> None:
        """
        Test that :func:`_check_sections` includes all requested sections."""
        # pylint: disable=protected-access
        result = handlers._check_sections(
            dummy_cfg,
            jobs=False,
            pipelines=True,
            sources=True,
            targets=True,
            transforms=True,
        )
        assert set(result) >= {'pipelines', 'sources', 'targets', 'transforms'}

    def test_check_sections_default(self, dummy_cfg: PipelineConfig) -> None:
        """
        Test that :func:`_check_sections` defaults to jobs when no flags are
        set.
        """
        # pylint: disable=protected-access
        result = handlers._check_sections(
            dummy_cfg,
            jobs=False,
            pipelines=False,
            sources=False,
            targets=False,
            transforms=False,
        )
        assert 'jobs' in result

    def test_pipeline_summary(self, dummy_cfg: PipelineConfig) -> None:
        """
        Test that :func:`_pipeline_summary` returns a mapping for a pipeline
        config.
        """
        # pylint: disable=protected-access

        summary = handlers._pipeline_summary(dummy_cfg)
        result: Mapping[str, object] = summary
        assert result['name'] == 'p1'
        assert result['version'] == 'v1'
        assert set(result) >= {'sources', 'targets', 'jobs'}


class TestCheckHandler:
    """Unit test suite for :func:`check_handler`."""

    def test_passes_substitute_flag(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: PipelineConfig,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`check_handler` forwards the substitute flag to config
        loader.
        """
        recorded: dict[str, object] = {}

        def fake_load_pipeline_config(
            path: str,
            substitute: bool,
        ) -> PipelineConfig:
            recorded['params'] = (path, substitute)
            return dummy_cfg

        monkeypatch.setattr(
            handlers,
            'load_pipeline_config',
            fake_load_pipeline_config,
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
        dummy_cfg: PipelineConfig,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`check_handler` prints requested sections."""
        monkeypatch.setattr(
            handlers,
            'load_pipeline_config',
            lambda path, substitute: dummy_cfg,
        )
        monkeypatch.setattr(
            handlers,
            '_check_sections',
            lambda _cfg, **_kwargs: {'targets': ['t1']},
        )
        assert handlers.check_handler(config='cfg.yml') == 0
        assert_emit_json(capture_io, {'targets': ['t1']}, pretty=True)


class TestExtractHandler:
    """Unit test suite for :func:`extract_handler`."""

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
    """Unit test suite for :func:`load_handler`."""

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
                'materialize_file_payload should not be called '
                'for STDIN sources',
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
    """Unit test suite for :func:`render_handler`."""

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
    """Unit test suite for :func:`run_handler`."""

    def test_emits_pipeline_summary_without_job(
        self,
        monkeypatch: pytest.MonkeyPatch,
        dummy_cfg: PipelineConfig,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`run_handler` emits a summary when no job set."""
        monkeypatch.setattr(
            handlers,
            'load_pipeline_config',
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
        dummy_cfg: PipelineConfig,
        capture_io: CaptureIo,
    ) -> None:
        """
        Test that :func:`run_handler` executes a named job and emits status.
        """
        monkeypatch.setattr(
            handlers,
            'load_pipeline_config',
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
    """Unit test suite for :func:`transform_handler`."""

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

    def test_writes_target_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that :func:`transform_handler` writes data to a target file."""
        # pylint: disable=unused-argument

        monkeypatch.setattr(
            handlers.cli_io,
            'resolve_cli_payload',
            lambda source, **_kwargs: {'source': source}
            if source == 'data.json'
            else {'select': ['id']},
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
        assert (
            'Data transformed and saved to out.json' in capsys.readouterr().out
        )


class TestValidateHandler:
    """Unit test suite for :func:`validate_handler`."""

    def test_emits_result_without_target(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capture_io: CaptureIo,
    ) -> None:
        """Test that :func:`validate_handler` emits results with no target."""
        monkeypatch.setattr(
            handlers.cli_io,
            'resolve_cli_payload',
            lambda source, **_kwargs: {'source': source}
            if source == 'data.json'
            else {'id': {'required': True}},
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
            lambda source, **_kwargs: {'source': source}
            if source == 'data.json'
            else {'id': {'required': True}},
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
            'Validation failed, no data to save for out.json'
            in capsys.readouterr().err
        )

    def test_writes_target_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that :func:`validate_handler` writes data to a target file."""
        monkeypatch.setattr(
            handlers.cli_io,
            'resolve_cli_payload',
            lambda source, **_kwargs: {'source': source}
            if source == 'data.json'
            else {'id': {'required': True}},
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
            'Validation result saved to',
        )


@pytest.mark.parametrize(
    ('kwargs', 'expected_keys'),
    (
        pytest.param(
            {'config': 'pipeline.yml', 'job': 'job1', 'pretty': True},
            ['config', 'job', 'pretty'],
            id='run-handler-smoke',
        ),
        pytest.param(
            {
                'source': 'data.json',
                'operations': '{"select": ["id"]}',
                'pretty': True,
            },
            ['source', 'operations', 'pretty'],
            id='transform-handler-smoke',
        ),
        pytest.param(
            {
                'source': 'data.json',
                'rules': '{"required": ["id"]}',
                'pretty': True,
            },
            ['source', 'rules', 'pretty'],
            id='validate-handler-smoke',
        ),
    ),
)
def test_handler_smoke(
    capture_handler: CaptureHandler,
    kwargs: dict[str, str | bool],
    expected_keys: list[str],
) -> None:
    """
    Smoke test CLI handlers to ensure they accept kwargs and call underlying
    logic.
    """
    # Map handler by keys
    if 'job' in kwargs:
        module, attr = handlers, 'run_handler'
    elif 'operations' in kwargs:
        module, attr = handlers, 'transform_handler'
    elif 'rules' in kwargs:
        module, attr = handlers, 'validate_handler'
    else:
        pytest.skip('Unknown handler')
        # Ensure function exits after skip to avoid unbound variable usage.
        return
    calls = capture_handler(module, attr)
    result = getattr(module, attr)(**kwargs)
    assert result == 0
    for key in expected_keys:
        assert key in calls
